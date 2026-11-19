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

// Package devicelease guards preallocated Azure managed disks used as composite
// origins. It is an internal provider boundary, not a DeviceProvider RPC server.
package devicelease

import (
	"context"
	"os"
	"regexp"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
)

// Binding is the immutable, provider-enrolled incarnation, never inferred from
// CSI volume context. Only the ARM lookup key is case-insensitive.
type Binding struct {
	AllocationID, CompositeID, ResourceID, DiskUniqueID string
	DeviceGeneration                                    uint64
	CapacityBytes                                       int64
}

type Owner struct {
	NodeID string
	Epoch  uint64
}

type Disposition uint8

const (
	_ Disposition = iota // unknown, including a zero record
	Ordinary
	Reserved
	Active
	Held
)

type Record struct {
	Disposition Disposition
	Binding     Binding
	Owner       Owner
}

// Attachment contains corroborating provider observations, not the physical
// fence itself. Mode is observed, not the mode requested in CSI volume context.
type Attachment struct {
	Disk               *armcompute.Disk
	VMResourceID, Mode string
}

// Authority provides ONE exclusion domain shared by enrollment, all CSI node
// and controller instances, and every ownership mutation. A local mutex, even
// with a persistent file, is not a production implementation.
type Authority interface {
	Begin(context.Context) (Session, error)
}

// Session keeps exclusion until End, including on cancellation. Lost sessions,
// async operations and ambiguous writes require durable quarantine/reconciliation;
// End is neither proof of completion nor permission to reuse a resource.
type Session interface {
	End()
	// Lookup must resolve aliases and use durable state and the provider clock.
	// Expired is Held, never Reserved. Ordinary means authoritatively outside
	// all allocations, not "missing from cache".
	Lookup(ctx context.Context, armKey string) (Record, error)
	// ProveOrdinary must bind the actual operation (including paths, LUNs,
	// name-only VM matches and QAD metadata) to the identified ordinary disks.
	// Unverified targets and modes must fail, even if the CSI handle is ordinary.
	ProveOrdinary(ctx context.Context, rpc proto.Message, armKeys []string) error
	// ProveExclusive must independently prove controller-driven exclusive
	// physical access for this owner, and return a fresh ARM disk and the VM
	// resource ID independently resolved for NodeID. ARM metadata alone is
	// not that proof. No QAD, shared disk, client assertion or timeout fallback.
	ProveExclusive(context.Context, Binding, Owner) (Attachment, error)
	// IdentifyOpened independently attests this already-open exclusive device
	// to this binding, generation and owner. Inspect the handle, not a path or
	// user-supplied dev_t. Return the observed Azure disk GUID and byte size.
	IdentifyOpened(context.Context, Binding, Owner, *os.File) (string, int64, error)
	// Retain durably commits the first owner and its liveness deadline before
	// success. An uncertain commit must remain unavailable until reconciled.
	Retain(context.Context, Binding, Owner) error
}

// Origin has no in-memory allocation or owner fallback. A nil driver option
// leaves normal CSI unchanged; an installed Origin without authority fails shut.
type Origin struct {
	authority Authority
}

func NewOrigin(a Authority) *Origin { return &Origin{authority: a} }

// Accept an unambiguous subset, rather than normalizing paths, URL escapes,
// empty segments, or malformed resource types into a different lookup key.
var diskID = regexp.MustCompile(`^/subscriptions/[a-z0-9_-]+/resourcegroups/[a-z0-9_().-]+/providers/microsoft\.compute/disks/[a-z0-9_.-]+$`)

func armKey(id string) (string, error) {
	key := strings.ToLower(id)
	if !diskID.MatchString(key) || strings.Contains(key, "/./") || strings.Contains(key, "/../") ||
		strings.HasSuffix(key, "/.") || strings.HasSuffix(key, "/..") || len(key) != len(id) {
		return "", status.Error(codes.InvalidArgument, "expected an unambiguous managed disk ARM resource ID")
	}
	return key, nil
}

func unavailable(err error) error {
	return status.Errorf(codes.Unavailable, "disk ownership or physical result unavailable: %v", err)
}

func (o *Origin) begin(ctx context.Context) (Session, error) {
	if o == nil || o.authority == nil {
		return nil, status.Error(codes.Unavailable, "no provider authority installed")
	}
	s, err := o.authority.Begin(ctx)
	if err != nil || s == nil {
		return nil, unavailable(err)
	}
	return s, nil
}

// BeginCSI admits a legacy physical operation. The caller MUST defer the
// returned End through all physical effects, not just through the lookup.
func (o *Origin) BeginCSI(ctx context.Context, rpc proto.Message, ids ...string) (func(), error) {
	if rpc == nil || len(ids) == 0 {
		return nil, status.Error(codes.InvalidArgument, "missing CSI operation or disk")
	}
	s, err := o.begin(ctx)
	if err != nil {
		return nil, err
	}
	admitted := false
	defer func() {
		if !admitted {
			s.End()
		}
	}()
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		key, err := armKey(id)
		if err != nil {
			return nil, err
		}
		record, err := s.Lookup(ctx, key)
		if err != nil {
			return nil, unavailable(err)
		}
		switch record.Disposition {
		case Ordinary:
			if record.Binding != (Binding{}) || record.Owner != (Owner{}) {
				return nil, status.Error(codes.Unavailable, "inconsistent ordinary disk record")
			}
		case Reserved, Active, Held:
			return nil, status.Error(codes.FailedPrecondition, "allocated origin unavailable to ordinary CSI")
		default:
			return nil, status.Error(codes.Unavailable, "disk classification unknown")
		}
		keys = append(keys, key)
	}
	if err := s.ProveOrdinary(ctx, rpc, keys); err != nil {
		return nil, unavailable(err)
	}
	admitted = true
	return s.End, nil
}

// Admit retains a first owner of a pre-enrolled disk, or checks an exact,
// still-live replay without extending liveness. It never replaces an owner.
// The authenticated caller/ledger layer is not implemented here.
func (o *Origin) Admit(ctx context.Context, b Binding, owner Owner) error {
	return o.withOwner(ctx, b, owner, nil, nil)
}

// WithOpenedDisk runs synchronous provider work on the same attested handle
// under the same exclusion as CSI and Admit. It cannot implicitly acquire.
// The trusted caller owns the handle and must not close/rebind it during the
// call, or use this admission as a grant for later IO.
func (o *Origin) WithOpenedDisk(ctx context.Context, b Binding, owner Owner, disk *os.File, use func(*os.File) error) error {
	if disk == nil || use == nil {
		return status.Error(codes.InvalidArgument, "missing opened disk or origin operation")
	}
	return o.withOwner(ctx, b, owner, disk, use)
}

func (o *Origin) withOwner(ctx context.Context, b Binding, owner Owner, disk *os.File, use func(*os.File) error) error {
	key, err := armKey(b.ResourceID)
	if err != nil {
		return err
	}
	if b.AllocationID == "" || b.CompositeID == "" || b.DiskUniqueID == "" || b.DeviceGeneration == 0 || b.CapacityBytes <= 0 || owner.NodeID == "" || owner.Epoch == 0 {
		return status.Error(codes.InvalidArgument, "incomplete origin or owner identity")
	}
	s, err := o.begin(ctx)
	if err != nil {
		return err
	}
	defer s.End()
	r, err := s.Lookup(ctx, key)
	if err != nil {
		return unavailable(err)
	}
	switch r.Disposition {
	case Ordinary:
		return status.Error(codes.FailedPrecondition, "disk is not a reserved origin")
	case Reserved, Active, Held:
		if r.Binding != b {
			return status.Error(codes.Aborted, "origin incarnation mismatch")
		}
	default:
		return status.Error(codes.Unavailable, "unknown origin state")
	}
	switch r.Disposition {
	case Reserved:
		if use != nil || r.Owner != (Owner{}) {
			return status.Error(codes.FailedPrecondition, "first acquisition only; no successor fencing proof")
		}
	case Active:
		if r.Owner != owner {
			return status.Error(codes.FailedPrecondition, "disk already retained by a different owner or generation")
		}
	case Held:
		return status.Error(codes.FailedPrecondition, "expired owner retained")
	}
	attachment, err := s.ProveExclusive(ctx, b, owner)
	if err != nil {
		return unavailable(err)
	}
	if err := verifyAttachment(b, attachment); err != nil {
		return err
	}
	if r.Disposition == Reserved {
		if err := s.Retain(ctx, b, owner); err != nil {
			return unavailable(err)
		}
		return nil
	}
	if use != nil {
		guid, size, err := s.IdentifyOpened(ctx, b, owner, disk)
		if err != nil || guid == "" || size <= 0 {
			return status.Error(codes.Unavailable, "opened disk identity not proven")
		}
		if guid != b.DiskUniqueID || size != b.CapacityBytes {
			return status.Error(codes.Aborted, "opened disk does not match enrolled origin")
		}
	}
	// Exclusion stops mutations, not the provider clock. Don't authorize a
	// replay or start work after liveness expired while proving attachment.
	current, err := s.Lookup(ctx, key)
	if err != nil {
		return unavailable(err)
	}
	if current.Disposition == Held {
		return status.Error(codes.FailedPrecondition, "expired owner retained")
	}
	if current != r {
		return status.Error(codes.Unavailable, "ownership changed during physical proof")
	}
	if use != nil {
		return use(disk)
	}
	return nil
}

func verifyAttachment(b Binding, a Attachment) error {
	disk, vmID := a.Disk, a.VMResourceID
	if disk == nil || disk.ID == nil || disk.Properties == nil || disk.Properties.UniqueID == nil ||
		*disk.Properties.UniqueID == "" || vmID == "" || disk.ManagedBy == nil || disk.Properties.DiskState == nil ||
		disk.Properties.DiskSizeBytes == nil || *disk.Properties.DiskSizeBytes <= 0 {
		return status.Error(codes.Unavailable, "incomplete provider disk evidence")
	}
	if !strings.EqualFold(*disk.ID, b.ResourceID) || *disk.Properties.UniqueID != b.DiskUniqueID || *disk.Properties.DiskSizeBytes != b.CapacityBytes {
		return status.Error(codes.Aborted, "observed disk is not the enrolled disk incarnation")
	}
	p := disk.Properties
	if a.Mode != azureconstants.AttachModeControllerDriven || !strings.EqualFold(*disk.ManagedBy, vmID) || *p.DiskState != armcompute.DiskStateAttached ||
		p.MaxShares == nil || *p.MaxShares != 1 || len(disk.ManagedByExtended) != 0 || len(p.ShareInfo) != 0 {
		return status.Error(codes.FailedPrecondition, "disk is not exclusively attached to the expected VM")
	}
	return nil
}
