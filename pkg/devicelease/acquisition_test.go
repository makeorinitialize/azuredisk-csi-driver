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
	"fmt"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"k8s.io/utils/ptr"
)

type modelAttach struct {
	vm       TargetVM
	dispatch int
}

func newModelAttach() *modelAttach {
	return &modelAttach{vm: TargetVM{ID: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/node", InstanceGUID: "vm-guid", ProvisioningState: "Succeeded"}}
}
func (m *modelAttach) Observe(context.Context, string) (TargetVM, error) { return m.vm, nil }
func (m *modelAttach) Dispatch(context.Context, string, *armcompute.Disk) error {
	m.dispatch++
	return context.DeadlineExceeded
}
func (m *modelAttach) complete(d *armcompute.Disk) {
	d.ManagedBy = ptr.To(m.vm.ID)
	d.Properties.DiskState = ptr.To(armcompute.DiskStateAttached)
	m.vm.DataDisks = []*armcompute.DataDisk{{ManagedDisk: &armcompute.ManagedDiskParameters{ID: ptr.To(resourceID)}, Lun: ptr.To[int32](2)}}
}

func acquisitionFixture(t *testing.T) (*reservationFixture, *Reservations, *modelAttach, attachInput) {
	f := fixture(t, "inventory-guid")
	f.disks.disk = &armcompute.Disk{ID: ptr.To(resourceID), Location: ptr.To(f.pool.Location), Properties: &armcompute.DiskProperties{
		ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateUnattached), UniqueID: ptr.To(f.pool.InventoryGUID), DiskSizeBytes: ptr.To(f.pool.Bytes),
		CreationData: &armcompute.CreationData{CreateOption: ptr.To(armcompute.DiskCreateOptionEmpty)},
	}}
	m := newModelAttach()
	r, err := f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	b, err := r.reserveTest(t.Context(), "reserve")
	require.NoError(t, err)
	return f, r, m, attachInput{Binding: b, Owner: Owner{NodeID: "node", Epoch: 7}}
}

type failSecondObservation struct {
	*modelAttach
	observations int
	fail         func(TargetVM) (TargetVM, error)
}

func (m *failSecondObservation) Observe(ctx context.Context, node string) (TargetVM, error) {
	m.observations++
	if m.observations == 2 {
		return m.fail(m.vm)
	}
	return m.modelAttach.Observe(ctx, node)
}

func TestControllerOriginPreparedRetry(t *testing.T) {
	for _, tc := range []struct {
		name string
		fail func(TargetVM) (TargetVM, error)
	}{
		{"read-timeout", func(TargetVM) (TargetVM, error) { return TargetVM{}, context.DeadlineExceeded }},
		{"updating", func(vm TargetVM) (TargetVM, error) { vm.ProvisioningState = "Updating"; return vm, nil }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			f, r, m, c := acquisitionFixture(t)
			r.attach = &failSecondObservation{modelAttach: m, fail: tc.fail}
			_, err := r.acquire(ctx, "key", c)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Zero(t, m.dispatch)

			// A fresh replica must keep the original intent and VM pin, even
			// though it can prove that no physical call has been made yet.
			r, err = f.adapter(t).WithControllerAttachment(m)
			require.NoError(t, err)
			newer := c
			newer.Owner.Epoch++
			_, err = r.acquire(ctx, "key", newer)
			require.Equal(t, codes.AlreadyExists, status.Code(err))
			_, err = r.acquire(ctx, "successor", newer)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Zero(t, m.dispatch)

			// The first real dispatch loses its reply. Subsequent retries may
			// observe completion but cannot dispatch again.
			_, err = r.acquire(ctx, "key", c)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, 1, m.dispatch)
			_, err = r.acquire(ctx, "key", c)
			require.Equal(t, codes.Unavailable, status.Code(err))
			m.complete(f.disks.disk)
			v, err := r.acquire(ctx, "key", c)
			require.NoError(t, err)
			require.Equal(t, c.Owner, v.Owner)
			require.Equal(t, 1, m.dispatch)
		})
	}
}

func TestControllerOriginPreparedReplacement(t *testing.T) {
	ctx := t.Context()
	f, r, m, c := acquisitionFixture(t)
	r.attach = &failSecondObservation{modelAttach: m, fail: func(vm TargetVM) (TargetVM, error) {
		vm.InstanceGUID = "replacement"
		vm.ProvisioningState = "Updating"
		return vm, nil
	}}
	_, err := r.acquire(ctx, "old", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = r.acquire(ctx, "fresh", c)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	r, err = f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	m.vm.InstanceGUID = "replacement"
	f.losePhase = "reserved" // retirement commits, but the reply is lost
	_, err = r.acquire(ctx, "old", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	r, err = f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	_, err = r.acquire(ctx, "old", c)
	require.Equal(t, codes.Aborted, status.Code(err))
	next := c
	next.Owner.Epoch++
	_, err = r.acquire(ctx, "old", next)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.reserveTest(ctx, "old")
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Zero(t, m.dispatch)
	_, err = r.acquire(ctx, "fresh", next)
	require.Equal(t, codes.Unavailable, status.Code(err))
	m.complete(f.disks.disk)
	got, err := r.acquire(ctx, "fresh", next)
	require.NoError(t, err)
	require.Equal(t, next.Owner, got.Owner)
	require.Equal(t, c.Binding, got.Binding)
	_, err = r.acquire(ctx, "old", c)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Equal(t, 1, m.dispatch)
}

func TestControllerOriginStaleRetirementCAS(t *testing.T) {
	for _, winner := range []string{"in-flight", "successor"} {
		t.Run(winner, func(t *testing.T) {
			ctx := t.Context()
			f, stale, m, c := acquisitionFixture(t)
			peer, err := f.adapter(t).WithControllerAttachment(m)
			require.NoError(t, err)
			key, claim := "old", c
			stale.attach = &failSecondObservation{modelAttach: m, fail: func(vm TargetVM) (TargetVM, error) {
				// Interleave a second replica between the stale reader's
				// prepared GET and its retirement CAS.
				vm.InstanceGUID = "replacement"
				if winner == "successor" {
					m.vm = vm
					_, err := peer.acquire(ctx, "old", c)
					require.Equal(t, codes.Aborted, status.Code(err))
					key = "fresh"
					claim.Owner.Epoch++
				}
				_, err := peer.acquire(ctx, key, claim)
				require.Equal(t, codes.Unavailable, status.Code(err))
				if winner == "successor" {
					m.complete(f.disks.disk)
					_, err = peer.acquire(ctx, key, claim)
					require.NoError(t, err)
				}
				return vm, nil
			}}
			_, err = stale.acquire(ctx, "old", c)
			require.Equal(t, codes.Aborted, status.Code(err))
			require.Equal(t, 1, f.conflicts)
			if winner == "in-flight" {
				m.complete(f.disks.disk)
			}
			got, err := peer.acquire(ctx, key, claim)
			require.NoError(t, err)
			require.Equal(t, claim.Owner, got.Owner)
			require.Equal(t, 1, m.dispatch)
		})
	}
}

func TestControllerOriginLostDispatchCAS(t *testing.T) {
	ctx := t.Context()
	f, r, m, c := acquisitionFixture(t)
	f.losePhase = "attaching"
	_, err := r.acquire(ctx, "key", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	r, err = f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	_, err = r.acquire(ctx, "key", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, m.dispatch, "ambiguous dispatch marker must not permit another dispatch")
}

func TestControllerOriginRejectedClaimsDoNotGrowJournal(t *testing.T) {
	ctx := t.Context()
	_, r, m, c := acquisitionFixture(t)
	checkNoGrowth := func(retained bool) {
		t.Helper()
		before, _, err := r.read(ctx)
		require.NoError(t, err)
		for i := range 64 {
			bad := c
			switch i % 3 {
			case 0:
				bad.Previous = uint64(i + 1)
			case 1:
				bad.Binding.DiskUniqueID = "wrong"
			case 2:
				bad.Binding.DeviceGeneration++
			}
			key := fmt.Sprintf("stale-%d", i)
			_, err = r.acquire(ctx, key, bad)
			require.Equal(t, codes.Aborted, status.Code(err))
			if retained {
				_, err = r.acquire(ctx, key, c)
				require.Equal(t, codes.FailedPrecondition, status.Code(err))
			}
		}
		after, _, err := r.read(ctx)
		require.NoError(t, err)
		require.Equal(t, before.ResourceVersion, after.ResourceVersion)
		require.Equal(t, before.Data, after.Data)
	}
	checkNoGrowth(false)
	require.Zero(t, m.dispatch)
	_, err := r.acquire(ctx, "good", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	checkNoGrowth(true)
	altered := c
	altered.Previous = 12
	_, err = r.acquire(ctx, "good", altered)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.acquire(ctx, "reserve", altered)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, 1, m.dispatch)
}

func TestControllerOriginLostAttachReply(t *testing.T) {
	ctx := t.Context()
	f, r, m, c := acquisitionFixture(t)
	v, err := r.acquire(ctx, "acquire", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, v)
	// No confirmation after a timeout; a restarted replica must only inspect.
	r, err = f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	_, err = r.acquire(ctx, "acquire", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	m.complete(f.disks.disk)
	f.losePhase = "active"
	_, err = r.acquire(ctx, "acquire", c)
	require.Equal(t, codes.Unavailable, status.Code(err))
	r, err = f.adapter(t).WithControllerAttachment(m)
	require.NoError(t, err)
	v, err = r.acquire(ctx, "acquire", c)
	require.NoError(t, err)
	require.Equal(t, c.Owner, v.Owner)
	replay, err := r.reserve(ctx, "reserve", "pool", "composite", 42)
	require.NoError(t, err)
	require.Equal(t, v, replay)
	newer := c
	newer.Owner.Epoch++
	_, err = r.acquire(ctx, "acquire", newer)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.acquire(ctx, "successor", newer)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = r.acquire(ctx, "reserve", c)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	// Simulate provider time reaching its deadline without sleeping.
	cm, l, err := r.read(ctx)
	require.NoError(t, err)
	l.RenewBy = 1
	ok, err := r.save(ctx, cm, l)
	require.NoError(t, err)
	require.True(t, ok)
	_, err = r.acquire(ctx, "acquire", c)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	held, err := r.lookup(ctx, c.Binding.AllocationID, nil)
	require.NoError(t, err)
	require.True(t, held.Held)
	require.Equal(t, c.Owner, held.Owner)
	require.Equal(t, 1, m.dispatch)
}

func TestControllerOriginRequiresCompletedAttachment(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before bool
		mutate func(*armcompute.Disk, *modelAttach)
	}{
		{"shared", true, func(d *armcompute.Disk, _ *modelAttach) { d.Properties.MaxShares = ptr.To[int32](2) }},
		{"shared-observation", true, func(d *armcompute.Disk, _ *modelAttach) { d.ManagedByExtended = []*string{ptr.To("another-vm")} }},
		{"recreated-disk", true, func(d *armcompute.Disk, _ *modelAttach) { d.Properties.UniqueID = ptr.To("replacement") }},
		{"preexisting-same-vm", true, func(d *armcompute.Disk, m *modelAttach) { m.complete(d) }},
		{"different-vm", false, func(d *armcompute.Disk, _ *modelAttach) { d.ManagedBy = ptr.To("another-vm") }},
		{"recreated-vm", false, func(_ *armcompute.Disk, m *modelAttach) { m.vm.InstanceGUID = "new-instance" }},
		{"disk-updating", false, func(d *armcompute.Disk, _ *modelAttach) { d.Properties.ProvisioningState = ptr.To("Updating") }},
		{"vm-updating", false, func(_ *armcompute.Disk, m *modelAttach) { m.vm.ProvisioningState = "Updating" }},
		{"stale-lun", false, func(_ *armcompute.Disk, m *modelAttach) { m.vm.DataDisks[0].ManagedDisk.ID = ptr.To(resourceID + "x") }},
		{"detaching", false, func(_ *armcompute.Disk, m *modelAttach) { m.vm.DataDisks[0].ToBeDetached = ptr.To(true) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, r, m, c := acquisitionFixture(t)
			if !tc.before {
				_, err := r.acquire(t.Context(), "a", c)
				require.Error(t, err)
				m.complete(f.disks.disk)
			}
			tc.mutate(f.disks.disk, m)
			v, err := r.acquire(t.Context(), "a", c)
			require.Error(t, err)
			require.Zero(t, v)
			if tc.before {
				require.Zero(t, m.dispatch)
			} else {
				require.Equal(t, 1, m.dispatch)
			}
		})
	}
}
