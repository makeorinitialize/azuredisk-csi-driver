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
	"regexp"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// ControllerAttachment uses normal Azure single-VM attachment for exclusion.
// Observe must read the actual VM resource, not a VolumeAttachment or LUN cache.
type ControllerAttachment interface {
	Observe(ctx context.Context, node string) (TargetVM, error)
	// Dispatch enters the normal attach path. No returned error proves that
	// Azure did not accept the operation.
	Dispatch(ctx context.Context, node string, disk *armcompute.Disk) error
}

type TargetVM struct {
	ID, InstanceGUID, ProvisioningState string
	DataDisks                           []*armcompute.DataDisk
}

var vmIDPattern = regexp.MustCompile(`(?i)^/subscriptions/[a-z0-9-]+/resourcegroups/[a-z0-9_.()-]+/providers/microsoft\.compute/(virtualmachines/[a-z0-9_-]+|virtualmachinescalesets/[a-z0-9_-]+/virtualmachines/[0-9]+)$`)

func validVMID(id string) bool { return vmIDPattern.MatchString(id) }

// WithControllerAttachment returns a separately configured adapter. It does
// not enable any CSI listener, node-side open, detach, or takeover.
func (r *Reservations) WithControllerAttachment(a ControllerAttachment) (*Reservations, error) {
	if a == nil || r == nil || r.pool.InventoryGUID == "" {
		return nil, status.Error(codes.FailedPrecondition, "controller attach requires pinned inventory")
	}
	out := *r
	out.attach = a
	return &out, nil
}

type attachInput struct {
	Binding  Binding
	Owner    Owner
	Previous uint64
}

func (c attachInput) valid() bool {
	b := c.Binding
	return validID(b.AllocationID) && validID(b.CompositeID) && validID(b.DiskUniqueID) && validID(b.ResourceID) && b.DeviceGeneration != 0 && b.CapacityBytes > 0 && validID(c.Owner.NodeID) && c.Owner.Epoch > 0
}

type ownerView struct {
	Binding
	Owner
	Until int64
	Held  bool
}

func (r *Reservations) binding(l *ledger) Binding {
	return Binding{AllocationID: r.allocationID(), CompositeID: l.Requests[l.First].Composite, ResourceID: r.pool.ResourceID, DiskUniqueID: l.GUID, DeviceGeneration: 1, CapacityBytes: r.pool.Bytes}
}

func (r *Reservations) diskForClaim(ctx context.Context, l *ledger) (*armcompute.Disk, error) {
	d, absent, err := r.get(ctx)
	if err != nil {
		return nil, err
	}
	if absent {
		return nil, status.Error(codes.Unavailable, "enrolled disk absent")
	}
	if err = r.verifyIdentity(d, l.GUID); err != nil {
		return nil, err
	}
	if d.Properties.PropertyUpdatesInProgress != nil {
		return nil, status.Error(codes.Unavailable, "disk update in progress")
	}
	return d, nil
}

// A VM's completed storage profile must agree with the disk RP. Never match
// by name or by LUN. Reject a pending detach and ambiguous duplicate entries.
func (vm TargetVM) hasDisk(id string) (bool, error) {
	if !validVMID(vm.ID) || vm.InstanceGUID == "" || !strings.EqualFold(vm.ProvisioningState, "Succeeded") {
		return false, status.Error(codes.Unavailable, "target VM identity or completed operation unconfirmed")
	}
	found := false
	luns := map[int32]bool{}
	for _, d := range vm.DataDisks {
		if d == nil || d.Lun == nil || *d.Lun < 0 || luns[*d.Lun] {
			return false, status.Error(codes.Unavailable, "incomplete VM disk list")
		}
		luns[*d.Lun] = true
		if d.ManagedDisk == nil || !strings.EqualFold(ptr.Deref(d.ManagedDisk.ID, ""), id) {
			if strings.EqualFold(ptr.Deref(d.Name, ""), id[strings.LastIndexByte(id, '/')+1:]) {
				return false, status.Error(codes.FailedPrecondition, "ambiguous same-name VM disk")
			}
			continue
		}
		if found || ptr.Deref(d.ToBeDetached, false) || d.DetachOption != nil || ptr.Deref(d.DeleteOption, armcompute.DiskDeleteOptionTypesDetach) != armcompute.DiskDeleteOptionTypesDetach {
			return false, status.Error(codes.Unavailable, "VM disk attachment incomplete or ambiguous")
		}
		found = true
	}
	return found, nil
}

func (r *Reservations) completed(ctx context.Context, l *ledger) error {
	if r.attach == nil {
		return status.Error(codes.Unavailable, "no controller attachment observer")
	}
	vm, err := r.attach.Observe(ctx, l.Claims[l.AttachKey].Owner.NodeID)
	if err != nil {
		return unavailable(err)
	}
	if !strings.EqualFold(vm.ID, l.TargetVM) || vm.InstanceGUID != l.TargetGUID {
		return status.Error(codes.Aborted, "target VM incarnation changed")
	}
	present, err := vm.hasDisk(r.pool.ResourceID)
	if err != nil {
		return err
	}
	d, err := r.diskForClaim(ctx, l)
	if err != nil {
		return err
	}
	if d.ManagedBy != nil && !strings.EqualFold(*d.ManagedBy, l.TargetVM) {
		return status.Error(codes.FailedPrecondition, "disk retained by another VM")
	}
	if !present || *d.Properties.DiskState != armcompute.DiskStateAttached || d.ManagedBy == nil {
		return status.Error(codes.Unavailable, "Azure attachment not confirmed")
	}
	return nil
}

func (r *Reservations) current(ctx context.Context, cm *v1.ConfigMap, l *ledger) (ownerView, error) {
	if l.Phase == "reserved" {
		b, err := r.inspectReserved(ctx, l)
		return ownerView{Binding: b}, err
	}
	if l.Phase != "active" && l.Phase != "held" {
		return ownerView{}, status.Error(codes.Unavailable, "origin operation unresolved")
	}
	v := ownerView{Binding: r.binding(l), Owner: l.Claims[l.AttachKey].Owner, Until: l.RenewBy, Held: l.Phase == "held"}
	if v.Held {
		return v, nil
	}
	if time.Now().Unix() < l.RenewBy {
		if err := r.completed(ctx, l); err != nil {
			return ownerView{}, err
		}
		if time.Now().Unix() < l.RenewBy {
			return v, nil
		}
	}
	l.Phase = "held"
	ok, err := r.save(ctx, cm, l)
	if err != nil {
		return ownerView{}, err
	}
	if !ok {
		return ownerView{}, status.Error(codes.Unavailable, "ownership changed")
	}
	v.Held = true
	return v, nil
}

func (r *Reservations) acquire(ctx context.Context, key string, c attachInput) (ownerView, error) {
	if r.attach == nil {
		return ownerView{}, status.Error(codes.Unimplemented, "controller acquisition disabled")
	}
	if !validID(key) || !c.valid() {
		return ownerView{}, status.Error(codes.InvalidArgument, "incomplete acquire intent")
	}
	var out ownerView
	err := r.within(ctx, func() error {
		for range 16 {
			cm, l, err := r.read(ctx)
			if err != nil {
				return err
			}
			if _, used := l.Requests[key]; used {
				return status.Error(codes.AlreadyExists, "request ID belongs to Reserve")
			}
			if old, ok := l.Claims[key]; ok {
				if old.attachInput != c {
					return status.Error(codes.AlreadyExists, "request ID bound to another acquire intent")
				}
				if old.Retired {
					return status.Error(codes.Aborted, "prepared request retired after target VM replacement")
				}
			}
			if l.Phase == "submitted" {
				return status.Error(codes.Unavailable, "reservation incomplete")
			}
			if c.Binding != r.binding(l) || c.Previous != 0 {
				return status.Error(codes.Aborted, "stale allocation or previous epoch")
			}
			if l.AttachKey != "" && l.AttachKey != key {
				return status.Error(codes.FailedPrecondition, "previous acquire intent or owner retained")
			}
			switch l.Phase {
			case "reserved", "attachPrepared":
				d, err := r.diskForClaim(ctx, l)
				if err != nil {
					return err
				}
				if *d.Properties.DiskState != armcompute.DiskStateUnattached || d.ManagedBy != nil {
					return status.Error(codes.FailedPrecondition, "cannot adopt or detach an existing attachment")
				}
				vm, err := r.attach.Observe(ctx, c.Owner.NodeID)
				if err != nil {
					return unavailable(err)
				}
				present, err := vm.hasDisk(c.Binding.ResourceID)
				if err != nil {
					return err
				}
				if present {
					return status.Error(codes.FailedPrecondition, "pre-existing VM attachment")
				}
				if l.Phase == "attachPrepared" && (!strings.EqualFold(vm.ID, l.TargetVM) || vm.InstanceGUID != l.TargetGUID) {
					if strings.EqualFold(vm.InstanceGUID, l.TargetGUID) {
						return status.Error(codes.Aborted, "target identity changed without proof of a replacement VM")
					}
					// Only this resourceVersion proves the exact intent is still
					// unsent. Never retry this reset against a newer snapshot.
					l.Claims[key] = attachClaim{attachInput: c, Retired: true}
					l.Phase, l.AttachKey, l.TargetVM, l.TargetGUID = "reserved", "", "", ""
					ok, err := r.save(ctx, cm, l)
					if err != nil {
						return err
					}
					if !ok {
						return status.Error(codes.Aborted, "prepared intent changed before retirement")
					}
					return status.Error(codes.Aborted, "prepared request retired after target VM replacement")
				}
				if l.Phase == "reserved" {
					// Bind the admitted key and both target identities atomically.
					// No dispatch is possible while the journal is in this phase.
					if l.Claims == nil {
						l.Claims = map[string]attachClaim{}
					}
					l.Claims[key] = attachClaim{attachInput: c}
					l.Phase, l.AttachKey, l.TargetVM, l.TargetGUID = "attachPrepared", key, vm.ID, vm.InstanceGUID
					if _, err = r.save(ctx, cm, l); err != nil {
						return err
					}
					continue
				}
				l.Phase = "attaching"
				ok, err := r.save(ctx, cm, l)
				if err != nil {
					return err
				}
				if ok {
					if err = r.attach.Dispatch(ctx, c.Owner.NodeID, d); err != nil {
						return unavailable(err)
					}
				}
			case "attaching":
				// Only the confirmed attachPrepared->attaching CAS winner dispatches.
				// Ambiguous requests reconcile from both RPs; never redrive an
				// unresolved attach against a new VM or detach a former owner.
				if err = r.completed(ctx, l); err != nil {
					return err
				}
				l.Phase, l.RenewBy = "active", time.Now().Add(time.Minute).Unix()
				if _, err = r.save(ctx, cm, l); err != nil {
					return err
				}
			case "active", "held":
				out, err = r.current(ctx, cm, l)
				if err != nil {
					return err
				}
				if out.Held {
					return status.Error(codes.FailedPrecondition, "owner expired and retained")
				}
				return nil
			default:
				return status.Error(codes.FailedPrecondition, "origin is not reserved")
			}
		}
		return status.Error(codes.Unavailable, "origin journal contention")
	})
	if err != nil {
		return ownerView{}, err
	}
	return out, nil
}
