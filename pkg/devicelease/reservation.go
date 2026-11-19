/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package devicelease

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient"
)

const allocationTag = "azuredisk-origin-allocation"

// ProvisioningScope must enforce a dedicated, never-attached slot, exclusively
// assigned to this journal incarnation for its entire life (including delayed
// ARM effects). No other journal, CSI client or cloud principal can create,
// replace, modify or attach it. Hold this guarantee across fn. This is NOT a
// lease on a previously used disk. Fail if exclusivity/provenance is uncertain.
type ProvisioningScope interface {
	Within(context.Context, types.UID, RawPool, func() error) error
	// For a new empty disk only: independently confirm allocatable capacity
	// for this exact slot. False,nil means KNOWN exhaustion.
	CanProvision(context.Context, RawPool) (bool, error)
}

type reservationDisks interface {
	Get(context.Context, string, string) (*armcompute.Disk, error)
	CreateOrUpdate(context.Context, string, string, armcompute.Disk) (*armcompute.Disk, error)
}

type DiskFactory interface {
	GetDiskClientForSub(string) (diskclient.Interface, error)
}

type Reservations struct {
	journal
	disks reservationDisks
	scope ProvisioningScope
}

// NewReservations uses a namespaced client and pins an existing journal by
// API-server UID. Use exactly one journal per provider, never one per replica.
func NewReservations(pool RawPool, maps coreclient.ConfigMapInterface, name string, uid types.UID, factory DiskFactory, scope ProvisioningScope) (*Reservations, error) {
	if !pool.valid() || maps == nil || name == "" || uid == "" || factory == nil {
		return nil, status.Error(codes.InvalidArgument, "incomplete origin reservation configuration")
	}
	if scope == nil {
		return nil, status.Error(codes.FailedPrecondition, "exclusive pre-attachment provisioning authority required")
	}
	disks, err := factory.GetDiskClientForSub(strings.Split(pool.ResourceID, "/")[2])
	if err != nil || disks == nil {
		return nil, unavailable(err)
	}
	return &Reservations{journal{maps, name, uid, pool}, disks, scope}, nil
}

func (r *Reservations) within(ctx context.Context, fn func() error) error {
	// Distinguish domain errors from a failed authority check, including one
	// after fn returned. Never return usable output on uncertain completion.
	var opErr error
	called := false
	err := r.scope.Within(ctx, r.uid, r.pool, func() error { called = true; opErr = fn(); return opErr })
	if err != nil && !errors.Is(err, opErr) {
		return unavailable(err)
	}
	if !called {
		return status.Error(codes.Unavailable, "origin provisioning authority not established")
	}
	return opErr
}

// Reserve returns a RESERVED binding only. Bytes is the original requested
// size, not the rounded slot size. Compatible intents converge by composite.
func (r *Reservations) Reserve(ctx context.Context, requestID, poolID, composite string, bytes uint64) (Binding, error) {
	if !validID(requestID) || !validID(poolID) || !validID(composite) || bytes == 0 {
		return Binding{}, status.Error(codes.InvalidArgument, "incomplete reserve intent")
	}
	intent := reserveIntent{"Reserve", poolID, composite, bytes}
	var result Binding
	err := r.within(ctx, func() error {
		for range 16 {
			cm, l, err := r.read(ctx)
			if err != nil {
				return err
			}
			if prev, ok := l.Requests[requestID]; ok {
				if prev != intent {
					return status.Error(codes.AlreadyExists, "request ID bound to a different input")
				}
			} else {
				l.Requests[requestID] = intent
				if _, err := r.save(ctx, cm, l); err != nil {
					return err
				}
				continue
			}
			first := l.Requests[l.First]
			if first.Composite == composite && first != intent {
				return status.Error(codes.AlreadyExists, "conflicting live reservation")
			}
			if poolID != r.pool.PoolID {
				return status.Error(codes.NotFound, "unknown pool")
			}
			if l.First != "" && first.Composite != composite {
				if l.Phase != "reserved" {
					return status.Error(codes.Unavailable, "origin provisioning unresolved")
				}
				if _, err := r.inspectReserved(ctx, l); err != nil {
					return err
				}
				return status.Error(codes.ResourceExhausted, "origin slot already allocated")
			}
			if bytes > uint64(r.pool.Bytes) {
				return status.Error(codes.ResourceExhausted, "request exceeds origin slot size")
			}
			switch l.Phase {
			case "free":
				l.First = requestID // committed only with confirmed enrollment or a create dispatch
				disk, absent, err := r.get(ctx)
				if err != nil {
					return err
				}
				if r.pool.InventoryGUID != "" {
					if absent {
						return status.Error(codes.Unavailable, "enrolled disk absent")
					}
					if err := r.verify(disk, r.pool.InventoryGUID); err != nil {
						return err
					}
					l.GUID, l.Phase = r.pool.InventoryGUID, "reserved"
					break
				}
				if !absent {
					return status.Error(codes.FailedPrecondition, "origin target already exists; refusing upsert or adoption")
				}
				ok, err := r.scope.CanProvision(ctx, r.pool)
				if err != nil {
					return unavailable(err)
				}
				if !ok {
					return status.Error(codes.ResourceExhausted, "confirmed insufficient origin capacity")
				}
				// Exactly the acknowledged CAS winner may issue the upsert.
				// Crash between CAS and PUT is indistinguishable from an
				// in-flight PUT: submitted retries may ONLY inspect.
				l.Phase = "submitted"
				committed, err := r.save(ctx, cm, l)
				if err != nil {
					return err
				}
				if committed {
					if err := r.create(ctx); err != nil {
						return err
					}
				}
				continue
			case "submitted":
				disk, absent, err := r.get(ctx)
				if err != nil {
					return err
				}
				if absent {
					return status.Error(codes.Unavailable, "origin create not yet confirmed; not resubmitting")
				}
				if err := r.verify(disk, ""); err != nil {
					return err
				}
				l.GUID, l.Phase = *disk.Properties.UniqueID, "reserved"
			case "reserved":
				result, err = r.inspectReserved(ctx, l)
				return err
			}
			if _, err := r.save(ctx, cm, l); err != nil {
				return err
			}
		}
		return status.Error(codes.Unavailable, "origin journal contention")
	})
	if err != nil {
		return Binding{}, err
	}
	return result, nil
}

// Capacity returns the confirmed reservable bytes. For a single slot this
// equals the maximum single allocation. Never count an in-flight create.
func (r *Reservations) Capacity(ctx context.Context, poolID string) (uint64, error) {
	if !validID(poolID) {
		return 0, status.Error(codes.InvalidArgument, "missing pool")
	}
	var n uint64
	err := r.within(ctx, func() error {
		_, l, err := r.read(ctx)
		if err != nil {
			return err
		}
		if poolID != r.pool.PoolID {
			return status.Error(codes.NotFound, "unknown pool")
		}
		if l.Phase == "reserved" {
			_, err = r.inspectReserved(ctx, l)
			return err
		}
		if l.Phase != "free" {
			return status.Error(codes.Unavailable, "origin provisioning unresolved")
		}
		disk, absent, err := r.get(ctx)
		if err != nil {
			return err
		}
		if r.pool.InventoryGUID != "" {
			if absent {
				return status.Error(codes.Unavailable, "enrolled disk absent")
			}
			if err := r.verify(disk, r.pool.InventoryGUID); err != nil {
				return err
			}
		} else {
			if !absent {
				return status.Error(codes.FailedPrecondition, "occupied origin target")
			}
			ok, err := r.scope.CanProvision(ctx, r.pool)
			if err != nil {
				return unavailable(err)
			}
			if !ok {
				return nil
			}
		}
		n = uint64(r.pool.Bytes)
		return nil
	})
	if err != nil {
		return 0, err
	}
	return n, nil
}

func (r *Reservations) Lookup(ctx context.Context, allocationID string) (Binding, error) {
	if !validID(allocationID) {
		return Binding{}, status.Error(codes.InvalidArgument, "missing allocation ID")
	}
	var b Binding
	err := r.within(ctx, func() error {
		_, l, err := r.read(ctx)
		if err != nil {
			return err
		}
		if l.Phase == "free" || allocationID != r.allocationID() {
			return status.Error(codes.NotFound, "no such allocation")
		}
		if l.Phase != "reserved" {
			return status.Error(codes.Unavailable, "origin provisioning unresolved")
		}
		b, err = r.inspectReserved(ctx, l)
		return err
	})
	if err != nil {
		return Binding{}, err
	}
	return b, nil
}

func (r *Reservations) get(ctx context.Context) (*armcompute.Disk, bool, error) {
	parts := strings.Split(r.pool.ResourceID, "/")
	d, err := r.disks.Get(ctx, parts[4], parts[8])
	var resp *azcore.ResponseError
	if errors.As(err, &resp) && resp.StatusCode == http.StatusNotFound && strings.EqualFold(resp.ErrorCode, "ResourceNotFound") {
		return nil, true, nil
	}
	if err != nil || d == nil {
		return nil, false, unavailable(err)
	}
	return d, false, nil
}

func (r *Reservations) create(ctx context.Context) error {
	parts := strings.Split(r.pool.ResourceID, "/")
	data, err := azureutils.GetValidCreationData(parts[2], parts[4], "", "")
	if err != nil {
		return unavailable(err)
	}
	model := armcompute.Disk{
		Location:   ptr.To(r.pool.Location),
		Tags:       map[string]*string{allocationTag: ptr.To(r.allocationID())},
		Properties: &armcompute.DiskProperties{CreationData: &data, DiskSizeGB: ptr.To(int32(r.pool.Bytes / gib)), MaxShares: ptr.To[int32](1)},
	}
	_, err = r.disks.CreateOrUpdate(ctx, parts[4], parts[8], model)
	if err != nil {
		return unavailable(err)
	}
	return nil
}

func (r *Reservations) verify(d *armcompute.Disk, guid string) error {
	p := d.Properties
	if p == nil || p.UniqueID == nil || *p.UniqueID == "" || p.ProvisioningState == nil || !strings.EqualFold(*p.ProvisioningState, "Succeeded") ||
		p.DiskState == nil || p.DiskSizeBytes == nil || p.CreationData == nil || p.CreationData.CreateOption == nil || d.Location == nil || d.ID == nil {
		return status.Error(codes.Unavailable, "incomplete origin disk observation")
	}
	if guid != "" && *p.UniqueID != guid {
		return status.Error(codes.Aborted, "stale Azure disk incarnation")
	}
	if !strings.EqualFold(*d.ID, r.pool.ResourceID) || !strings.EqualFold(*d.Location, r.pool.Location) || *p.DiskSizeBytes != r.pool.Bytes ||
		*p.CreationData.CreateOption != armcompute.DiskCreateOptionEmpty ||
		(r.pool.InventoryGUID == "" && ptr.Deref(d.Tags[allocationTag], "") != r.allocationID()) {
		return status.Error(codes.FailedPrecondition, "origin provenance or disk incarnation mismatch")
	}
	if *p.DiskState != armcompute.DiskStateUnattached || d.ManagedBy != nil || len(d.ManagedByExtended) != 0 ||
		len(p.ShareInfo) != 0 || ptr.Deref(p.MaxShares, 1) != 1 {
		return status.Error(codes.FailedPrecondition, "disk is not an unused exclusive origin")
	}
	return nil
}

func (r *Reservations) inspectReserved(ctx context.Context, l *ledger) (Binding, error) {
	d, absent, err := r.get(ctx)
	if err != nil {
		return Binding{}, err
	}
	if absent {
		return Binding{}, status.Error(codes.Unavailable, "reserved origin unavailable")
	}
	if err := r.verify(d, l.GUID); err != nil {
		return Binding{}, err
	}
	return Binding{AllocationID: r.allocationID(), CompositeID: l.Requests[l.First].Composite, ResourceID: r.pool.ResourceID, DiskUniqueID: l.GUID,
		DeviceGeneration: 1, CapacityBytes: r.pool.Bytes}, nil
}
