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
	"os"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
)

const resourceID = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk"

var (
	binding      = Binding{"allocation", "composite", resourceID, "disk-instance", 4, 1 << 30}
	owner        = Owner{"node", 7}
	errUncertain = errors.New("unknown outcome")
)

type fakeAuthority struct {
	lock                           chan struct{}
	record                         Record
	attachment                     Attachment
	lookupErr, proofErr, commitErr error
	expireDuringProof              bool
	openedGUID                     string
	openedBytes                    int64
	observedHandle                 *os.File
	writes                         int
}

func newAuthority() *fakeAuthority {
	vmID := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm"
	return &fakeAuthority{
		lock:       make(chan struct{}, 1),
		record:     Record{Disposition: Reserved, Binding: binding},
		openedGUID: binding.DiskUniqueID, openedBytes: binding.CapacityBytes,
		attachment: Attachment{Mode: azureconstants.AttachModeControllerDriven, VMResourceID: vmID, Disk: &armcompute.Disk{
			ID: ptr.To(resourceID), ManagedBy: &vmID, Properties: &armcompute.DiskProperties{
				UniqueID: ptr.To(binding.DiskUniqueID), DiskState: ptr.To(armcompute.DiskStateAttached), MaxShares: ptr.To[int32](1),
				DiskSizeBytes: ptr.To(binding.CapacityBytes),
			},
		}},
	}
}

func (a *fakeAuthority) Begin(ctx context.Context) (Session, error) {
	select {
	case a.lock <- struct{}{}:
		return a, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (a *fakeAuthority) End() { <-a.lock }
func (a *fakeAuthority) Lookup(_ context.Context, key string) (Record, error) {
	if key != strings.ToLower(resourceID) {
		return Record{}, errors.New("unclassified key")
	}
	return a.record, a.lookupErr
}
func (a *fakeAuthority) ProveOrdinary(context.Context, proto.Message, []string) error {
	return a.proofErr
}
func (a *fakeAuthority) ProveExclusive(context.Context, Binding, Owner) (Attachment, error) {
	if a.expireDuringProof {
		a.record.Disposition = Held
	}
	return a.attachment, a.proofErr
}
func (a *fakeAuthority) Retain(_ context.Context, _ Binding, o Owner) error {
	a.writes++
	a.record.Disposition, a.record.Owner = Active, o
	return a.commitErr // model a committed effect with lost reply
}
func (a *fakeAuthority) IdentifyOpened(_ context.Context, _ Binding, _ Owner, f *os.File) (string, int64, error) {
	a.observedHandle = f
	return a.openedGUID, a.openedBytes, nil
}

func TestOriginRetainedAcrossReplayAndUncertainty(t *testing.T) {
	ctx := t.Context()
	a := newAuthority()
	o := NewOrigin(a)
	a.commitErr = errUncertain
	require.Equal(t, codes.Unavailable, status.Code(o.Admit(ctx, binding, owner)))
	// A fresh adapter consults the authority; it cannot start empty.
	o = NewOrigin(a)
	require.NoError(t, o.Admit(ctx, binding, owner))
	for _, conflict := range []Owner{{"other-node", owner.Epoch}, {owner.NodeID, owner.Epoch + 1}} {
		require.Equal(t, codes.FailedPrecondition, status.Code(o.Admit(ctx, binding, conflict)))
	}
	for _, mutate := range []func(*Binding){
		func(b *Binding) { b.CompositeID += "x" }, func(b *Binding) { b.AllocationID += "x" },
		func(b *Binding) { b.DeviceGeneration++ }, func(b *Binding) { b.DiskUniqueID += "x" },
		func(b *Binding) { b.ResourceID = strings.ToLower(b.ResourceID) },
	} {
		stale := binding
		mutate(&stale)
		require.Equal(t, codes.Aborted, status.Code(o.Admit(ctx, stale, owner)))
	}
	a.lookupErr = errUncertain
	require.Equal(t, codes.Unavailable, status.Code(o.Admit(ctx, binding, Owner{"successor", 8})))
	a.lookupErr = nil
	a.proofErr = errUncertain
	require.Equal(t, codes.Unavailable, status.Code(o.Admit(ctx, binding, owner)))
	a.proofErr = nil
	a.expireDuringProof = true
	require.Equal(t, codes.FailedPrecondition, status.Code(o.Admit(ctx, binding, owner)))
	require.Equal(t, codes.FailedPrecondition, status.Code(o.Admit(ctx, binding, Owner{"successor", 8})))
	require.Equal(t, owner, a.record.Owner)
	require.Equal(t, 1, a.writes)
}

func TestOriginUseOnlyAttestedHandle(t *testing.T) {
	a := newAuthority()
	o := NewOrigin(a)
	// A stand-in handle; no real device is opened or probed.
	f, err := os.Open("origin_test.go")
	require.NoError(t, err)
	defer f.Close()
	use := func(got *os.File) error {
		require.Same(t, f, got)
		require.Same(t, a.observedHandle, got)
		require.Len(t, a.lock, 1)
		return nil
	}
	ctx := t.Context()
	require.Equal(t, codes.FailedPrecondition, status.Code(o.WithOpenedDisk(ctx, binding, owner, f, use)))
	require.NoError(t, o.Admit(ctx, binding, owner))
	require.NoError(t, o.WithOpenedDisk(ctx, binding, owner, f, use))
	never := func(*os.File) error { t.Fatal("unproven handle admitted"); return nil }
	a.openedGUID = "different-disk"
	require.Equal(t, codes.Aborted, status.Code(o.WithOpenedDisk(ctx, binding, owner, f, never)))
	a.openedGUID = binding.DiskUniqueID
	a.openedBytes++
	require.Equal(t, codes.Aborted, status.Code(o.WithOpenedDisk(ctx, binding, owner, f, never)))
	a.openedBytes = 0
	require.Equal(t, codes.Unavailable, status.Code(o.WithOpenedDisk(ctx, binding, owner, f, never)))
	require.Equal(t, owner, a.record.Owner)
}

func TestOriginRejectsUnprovenAttachment(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*fakeAuthority)
		code   codes.Code
	}{
		{"missing disk", func(a *fakeAuthority) { a.attachment.Disk = nil }, codes.Unavailable},
		{"other resource", func(a *fakeAuthority) { a.attachment.Disk.ID = ptr.To(resourceID + "x") }, codes.Aborted},
		{"recreated disk", func(a *fakeAuthority) { a.attachment.Disk.Properties.UniqueID = ptr.To("new") }, codes.Aborted},
		{"missing stable identity", func(a *fakeAuthority) { a.attachment.Disk.Properties.UniqueID = nil }, codes.Unavailable},
		{"other VM", func(a *fakeAuthority) { a.attachment.Disk.ManagedBy = ptr.To("other") }, codes.FailedPrecondition},
		{"unattached", func(a *fakeAuthority) {
			a.attachment.Disk.Properties.DiskState = ptr.To(armcompute.DiskStateUnattached)
		}, codes.FailedPrecondition},
		{"shared", func(a *fakeAuthority) { a.attachment.Disk.Properties.MaxShares = ptr.To[int32](2) }, codes.FailedPrecondition},
		{"additional owner", func(a *fakeAuthority) { a.attachment.Disk.ManagedByExtended = []*string{ptr.To("other")} }, codes.FailedPrecondition},
		{"QAD", func(a *fakeAuthority) { a.attachment.Mode = azureconstants.AttachModeNodeDriven }, codes.FailedPrecondition},
		{"no fence", func(a *fakeAuthority) { a.proofErr = errUncertain }, codes.Unavailable},
		{"reserved with high water", func(a *fakeAuthority) { a.record.Owner.Epoch = 2 }, codes.FailedPrecondition},
		{"unknown state", func(a *fakeAuthority) { a.record.Disposition = 0 }, codes.Unavailable},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := newAuthority()
			tc.change(a)
			require.Equal(t, tc.code, status.Code(NewOrigin(a).Admit(t.Context(), binding, owner)))
			require.Zero(t, a.writes)
		})
	}
}

func TestOriginCSINeverInfersOrdinary(t *testing.T) {
	ctx := t.Context()
	a := newAuthority()
	rpc := &csi.NodeUnstageVolumeRequest{VolumeId: strings.ToUpper(resourceID)}
	o := NewOrigin(a)
	for _, state := range []Disposition{Reserved, Active, Held} {
		a.record.Disposition = state
		end, err := o.BeginCSI(ctx, rpc, rpc.VolumeId)
		require.Nil(t, end)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	}
	a.record = Record{Disposition: Ordinary}
	for _, id := range []string{"disk", resourceID + "/..", resourceID + "%2fother", strings.Replace(resourceID, "/disks/", "/snapshots/", 1)} {
		_, err := o.BeginCSI(ctx, rpc, id)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	a.proofErr = errUncertain
	_, err := o.BeginCSI(ctx, rpc, rpc.VolumeId)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = NewOrigin(nil).BeginCSI(ctx, rpc, rpc.VolumeId)
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestOriginPhysicalOperationExcludesLeaseChange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		a := newAuthority()
		a.record = Record{Disposition: Ordinary}
		end, err := NewOrigin(a).BeginCSI(t.Context(), &csi.ControllerUnpublishVolumeRequest{VolumeId: resourceID}, resourceID)
		require.NoError(t, err)
		result := make(chan error, 1)
		go func() {
			// A separate provider instance wants to enroll and acquire this disk.
			session, err := a.Begin(t.Context())
			if err != nil {
				result <- err
				return
			}
			a.record = Record{Disposition: Reserved, Binding: binding}
			session.End()
			result <- NewOrigin(a).Admit(t.Context(), binding, owner)
		}()
		synctest.Wait()
		require.Equal(t, Ordinary, a.record.Disposition)
		require.Empty(t, result)
		end() // actual detach has now returned
		require.NoError(t, <-result)
		require.Equal(t, owner, a.record.Owner)
		_, err = NewOrigin(a).BeginCSI(t.Context(), &csi.DeleteVolumeRequest{VolumeId: resourceID}, resourceID)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}
