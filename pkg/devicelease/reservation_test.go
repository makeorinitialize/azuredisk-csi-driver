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
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient"
)

type testDisks struct {
	diskclient.Interface // no fallback for delete, patch, etc.
	mu                   sync.Mutex
	disk                 *armcompute.Disk
	creates              int
	getErr, putErr       error
}

func (c *testDisks) GetDiskClientForSub(sub string) (diskclient.Interface, error) {
	if sub != "sub" {
		return nil, errors.New("wrong subscription")
	}
	return c, nil
}
func (c *testDisks) Get(_ context.Context, rg, name string) (*armcompute.Disk, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if rg != "rg" || name != "disk" {
		return nil, errors.New("wrong target")
	}
	if c.getErr != nil {
		return nil, c.getErr
	}
	if c.disk == nil {
		return nil, &azcore.ResponseError{StatusCode: http.StatusNotFound, ErrorCode: "ResourceNotFound"}
	}
	return c.disk, nil
}
func (c *testDisks) CreateOrUpdate(_ context.Context, rg, name string, d armcompute.Disk) (*armcompute.Disk, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.creates++
	if rg != "rg" || name != "disk" || c.disk != nil {
		return nil, errors.New("unsafe upsert")
	}
	d.ID = ptr.To(resourceID)
	d.Properties.UniqueID = ptr.To("azure-guid")
	d.Properties.ProvisioningState = ptr.To("Succeeded")
	d.Properties.DiskState = ptr.To(armcompute.DiskStateUnattached)
	d.Properties.DiskSizeBytes = ptr.To(int64(*d.Properties.DiskSizeGB) * gib)
	c.disk = &d
	if c.putErr != nil {
		return nil, c.putErr
	}
	return c.disk, nil
}

type testScope struct {
	available      bool
	err            error
	beforeCapacity func()
}

func (s *testScope) Within(_ context.Context, _ types.UID, _ RawPool, fn func() error) error {
	if s.err != nil {
		return s.err
	}
	return fn()
}
func (s *testScope) CanProvision(context.Context, RawPool) (bool, error) {
	if s.beforeCapacity != nil {
		s.beforeCapacity()
	}
	return s.available, nil
}

type reservationFixture struct {
	client    *fake.Clientset
	disks     *testDisks
	scope     *testScope
	pool      RawPool
	losePhase string
	conflicts int
}

func fixture(t *testing.T, inventory string) *reservationFixture {
	t.Helper()
	f := &reservationFixture{
		disks: &testDisks{}, scope: &testScope{available: true},
		pool: RawPool{Provider: "provider", PoolID: "pool", ResourceID: resourceID, Location: "westus", Bytes: gib, InventoryGUID: inventory},
	}
	data, err := InitialJournal(f.pool)
	require.NoError(t, err)
	f.client = fake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "journal", Namespace: "ns", UID: "initial-uid", ResourceVersion: "1"}, Data: data,
	})
	// client-go's object tracker does not enforce API-server resourceVersion.
	f.client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		next := action.(ktesting.UpdateAction).GetObject().(*corev1.ConfigMap).DeepCopy()
		gvr := action.GetResource()
		old, err := f.client.Tracker().Get(gvr, next.Namespace, next.Name)
		if err != nil {
			return true, nil, err
		}
		prev := old.(*corev1.ConfigMap)
		if next.UID != prev.UID || next.ResourceVersion != prev.ResourceVersion {
			f.conflicts++
			return true, nil, apierrors.NewConflict(gvr.GroupResource(), next.Name, errors.New("stale RV"))
		}
		version, err := strconv.Atoi(next.ResourceVersion)
		require.NoError(t, err)
		next.ResourceVersion = strconv.Itoa(version + 1)
		require.NoError(t, f.client.Tracker().Update(gvr, next, next.Namespace))
		var l ledger
		require.NoError(t, json.Unmarshal([]byte(next.Data[journalKey]), &l))
		if f.losePhase != "" && l.Phase == f.losePhase {
			f.losePhase = ""
			return true, nil, errUncertain
		}
		return true, next, nil
	})
	return f
}
func (f *reservationFixture) adapter(t *testing.T) *Reservations {
	t.Helper()
	r, err := NewReservations(f.pool, f.client.CoreV1().ConfigMaps("ns"), "journal", "initial-uid", f.disks, f.scope)
	require.NoError(t, err)
	return r
}
func (r *Reservations) reserveTest(ctx context.Context, key string) (Binding, error) {
	return r.Reserve(ctx, key, "pool", "composite", 42)
}

func TestRawReservationLostReplies(t *testing.T) {
	ctx := t.Context()
	f := fixture(t, "")
	r := f.adapter(t)
	n, err := r.Capacity(ctx, "pool")
	require.NoError(t, err)
	require.Equal(t, uint64(gib), n)
	f.disks.putErr = errUncertain
	b, err := r.reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, b)
	n, err = r.Capacity(ctx, "pool")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, n)
	f.losePhase = "reserved"
	b, err = f.adapter(t).reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, b)
	r = f.adapter(t)
	b, err = r.reserveTest(ctx, "request")
	require.NoError(t, err)
	require.Equal(t, f.pool.Bytes, b.CapacityBytes)
	require.Equal(t, "azure-guid", b.DiskUniqueID)
	same, err := r.reserveTest(ctx, "alias")
	require.NoError(t, err)
	require.Equal(t, b, same)
	same, err = r.Lookup(ctx, b.AllocationID)
	require.NoError(t, err)
	require.Equal(t, b, same)
	n, err = r.Capacity(ctx, "pool")
	require.NoError(t, err)
	require.Zero(t, n)
	_, err = r.Reserve(ctx, "request", "other-pool", "composite", 42)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.Reserve(ctx, "alias", "pool", "other", 42)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.Reserve(ctx, "different-size", "pool", "composite", 43)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.Reserve(ctx, "different-pool", "other-pool", "composite", 42)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	_, err = r.Reserve(ctx, "second", "pool", "other", 42)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, 1, f.disks.creates)
	// Missing/replaced disk is never recreated/adopted after commit.
	f.disks.disk.Properties.UniqueID = ptr.To("replacement")
	_, err = r.Lookup(ctx, b.AllocationID)
	require.Equal(t, codes.Aborted, status.Code(err))
	f.disks.disk = nil
	_, err = r.reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, 1, f.disks.creates)
}

func TestRawReservationAmbiguousDispatch(t *testing.T) {
	ctx := t.Context()
	f := fixture(t, "")
	f.losePhase = "submitted"
	_, err := f.adapter(t).reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	r := f.adapter(t)
	_, err = r.reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Zero(t, f.disks.creates)
	_, err = r.Lookup(ctx, r.allocationID())
	require.Equal(t, codes.Unavailable, status.Code(err))
}

func TestRawReservationCAS(t *testing.T) {
	ctx := t.Context()
	f := fixture(t, "")
	first, second := f.adapter(t), f.adapter(t)
	entered, release := make(chan struct{}, 2), make(chan struct{})
	f.scope.beforeCapacity = func() { entered <- struct{}{}; <-release }
	type result struct {
		b   Binding
		err error
	}
	replies := make(chan result, 2)
	for idx, r := range []*Reservations{first, second} {
		go func() { b, err := r.reserveTest(ctx, strconv.Itoa(idx)); replies <- result{b, err} }()
	}
	// Both saw an unused, absent disk before either could commit dispatch.
	<-entered
	<-entered
	close(release)
	for range 2 {
		got := <-replies
		if got.err != nil {
			require.Equal(t, codes.Unavailable, status.Code(got.err))
		}
	}
	b, err := first.reserveTest(ctx, "0")
	require.NoError(t, err)
	other, err := second.reserveTest(ctx, "1")
	require.NoError(t, err)
	require.Equal(t, b, other)
	require.Positive(t, f.conflicts)
	require.Equal(t, 1, f.disks.creates)
}

func TestRawReservationInventory(t *testing.T) {
	ctx := t.Context()
	for _, tc := range []struct {
		name, pin string
		mutate    func(*armcompute.Disk)
		allowed   bool
	}{
		{"existing unpinned disk", "", func(*armcompute.Disk) {}, false},
		{"wrong pin", "other", func(*armcompute.Disk) {}, false},
		{"imported", "pin", func(d *armcompute.Disk) {
			d.Properties.CreationData.CreateOption = ptr.To(armcompute.DiskCreateOptionImport)
		}, false},
		{"attached", "pin", func(d *armcompute.Disk) { d.ManagedBy = ptr.To("vm") }, false},
		{"enrolled inventory", "pin", func(*armcompute.Disk) {}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := fixture(t, tc.pin)
			f.disks.disk = &armcompute.Disk{ID: ptr.To(resourceID), Location: ptr.To(f.pool.Location), Properties: &armcompute.DiskProperties{
				ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateUnattached),
				UniqueID: ptr.To("pin"), DiskSizeBytes: ptr.To(f.pool.Bytes), CreationData: &armcompute.CreationData{CreateOption: ptr.To(armcompute.DiskCreateOptionEmpty)},
			}}
			tc.mutate(f.disks.disk)
			r := f.adapter(t)
			n, err := r.Capacity(ctx, "pool")
			if tc.allowed {
				require.NoError(t, err)
				require.Equal(t, uint64(gib), n)
			} else {
				require.Error(t, err)
			}
			b, err := r.reserveTest(ctx, "key")
			if tc.allowed {
				require.NoError(t, err)
				require.Equal(t, "pin", b.DiskUniqueID)
			} else {
				require.Error(t, err)
				require.Zero(t, b)
			}
			require.Zero(t, f.disks.creates)
		})
	}
}

func TestRawReservationAuthorityUnavailable(t *testing.T) {
	ctx := t.Context()
	f := fixture(t, "")
	r := f.adapter(t)
	f.scope.available = false
	n, err := r.Capacity(ctx, "pool")
	require.NoError(t, err)
	require.Zero(t, n)
	_, err = r.reserveTest(ctx, "request")
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	f.disks.getErr = errUncertain
	_, err = r.Capacity(ctx, "pool")
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.disks.getErr = &azcore.ResponseError{StatusCode: 404, ErrorCode: "SubscriptionNotFound"}
	_, err = r.reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.disks.getErr = nil
	f.scope.err = errUncertain
	_, err = r.Lookup(ctx, "absent")
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.scope.err = nil
	maps := f.client.CoreV1().ConfigMaps("ns")
	old, err := maps.Get(ctx, "journal", metav1.GetOptions{})
	require.NoError(t, err)
	require.NoError(t, maps.Delete(ctx, "journal", metav1.DeleteOptions{}))
	_, err = r.Lookup(ctx, "absent")
	require.Equal(t, codes.Unavailable, status.Code(err))
	old.UID = "recreated"
	_, err = maps.Create(ctx, old, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = r.reserveTest(ctx, "request")
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = NewReservations(f.pool, maps, "journal", "recreated", f.disks, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Zero(t, f.disks.creates)
}

func TestRawReservationUnrecognizedJournal(t *testing.T) {
	for _, alter := range []func(string) string{
		func(s string) string { return strings.Replace(s, `"Version":1`, `"Version":2`, 1) },
		func(s string) string {
			return strings.Replace(s, `"Phase":"free"`, `"Phase":"submitted","Phase":"free"`, 1)
		},
		func(s string) string { return strings.Replace(s, `"Phase":"free"`, `"Phase":""`, 1) },
	} {
		f := fixture(t, "")
		r := f.adapter(t)
		cm, err := r.maps.Get(t.Context(), "journal", metav1.GetOptions{})
		require.NoError(t, err)
		cm.Data[journalKey] = alter(cm.Data[journalKey])
		_, err = r.maps.Update(t.Context(), cm, metav1.UpdateOptions{})
		require.NoError(t, err)
		_, err = r.reserveTest(t.Context(), "key")
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.Zero(t, f.disks.creates)
	}
}
