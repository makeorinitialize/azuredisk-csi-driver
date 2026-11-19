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
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

type blockRenewFixture struct {
	f              *reservationFixture
	h              *BlockHold
	d              *armcompute.Disk
	vm             TargetVM
	observe        func()
	client         pb.DeviceProviderClient
	request        *pb.RenewRequest
	ca, clientCert tls.Certificate
}

func newBlockRenewFixture(t *testing.T) *blockRenewFixture {
	t.Helper()
	f := fixture(t, "pin")
	vm := newModelAttach()
	s := BlockSelection{Provider: "Origin", PoolID: "ordinary", Binding: binding, Owner: owner, PV: "pv", PVUID: "pvuid", PVC: "pvc", PVCUID: "pvcuid", Namespace: "ns", VMID: vm.vm.ID, VMGUID: vm.vm.InstanceGUID}
	data, err := InitialBlockHold(s)
	require.NoError(t, err)
	data["legacy-extension"] = "must survive all CAS transitions"
	maps := f.client.CoreV1().ConfigMaps("ns")
	_, err = maps.Create(t.Context(), &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ordinary", UID: "one-use", ResourceVersion: "1"}, Data: data}, metav1.CreateOptions{})
	require.NoError(t, err)
	h, err := NewBlockHold(s, maps, "ordinary", "one-use", blockReceipt{s})
	require.NoError(t, err)
	done, err := h.BeginSetup(t.Context(), true)
	require.NoError(t, err)
	require.NoError(t, done())
	d := &armcompute.Disk{ID: &s.ResourceID, Properties: &armcompute.DiskProperties{ProvisioningState: ptr.To("Succeeded"), UniqueID: &s.DiskUniqueID, DiskSizeBytes: &s.CapacityBytes, MaxShares: ptr.To[int32](1)}}
	vm.complete(d)
	ca := testCert(t, 1, nil, false)
	r := &blockRenewFixture{f: f, h: h, d: d, vm: vm.vm, ca: ca, clientCert: testCert(t, 2, &ca, false)}
	r.client = r.connect(t, ReservationNode, s.NodeID)
	allocation := (&publishedRPC{h: h}).allocation()
	r.request = &pb.RenewRequest{RequestId: "renew", Owner: &pb.Owner{Allocation: allocation, NodeId: s.NodeID, OwnerEpoch: s.Epoch}}
	_, err = r.client.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire", Owner: r.request.Owner})
	require.NoError(t, err)
	// Legacy one-shot state with a still-live, near deadline, so the first
	// renewal must extend rather than merely round to the same second.
	r.edit(t, func(cm *v1.ConfigMap) {
		cm.Data["deadline"] = strconv.FormatInt(time.Now().Add(20*time.Second).Unix(), 10)
	})
	return r
}

func (r *blockRenewFixture) connect(t *testing.T, role ReservationRole, node string) pb.DeviceProviderClient {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(r.ca.Leaf)
	server, err := NewPublishedBlockServer(r.h, func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error) {
		if r.observe != nil {
			r.observe()
		}
		return r.d, r.vm, nil
	}, testCert(t, 3, &r.ca, true), roots,
		[]ReservationGrant{{sha256.Sum256(r.clientCert.Certificate[0]), role, []string{r.h.s.CompositeID}, node}})
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "origin.provider.example", Certificates: []tls.Certificate{r.clientCert}})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewDeviceProviderClient(conn)
}

func (r *blockRenewFixture) state(t *testing.T) *v1.ConfigMap {
	t.Helper()
	cm, err := r.h.maps.Get(t.Context(), r.h.name, metav1.GetOptions{})
	require.NoError(t, err)
	return cm
}

func (r *blockRenewFixture) edit(t *testing.T, change func(*v1.ConfigMap)) {
	t.Helper()
	cm := r.state(t)
	change(cm)
	_, err := r.h.maps.Update(t.Context(), cm, metav1.UpdateOptions{})
	require.NoError(t, err)
}

func (r *blockRenewFixture) restart(t *testing.T) {
	t.Helper()
	h, err := NewBlockHold(r.h.s, r.h.maps, r.h.name, r.h.uid, r.h.cleanup)
	require.NoError(t, err)
	r.h = h
	r.client = r.connect(t, ReservationNode, h.s.NodeID)
}

func (r *blockRenewFixture) release(t *testing.T, key string) error {
	t.Helper()
	_, err := r.client.Release(t.Context(), &pb.ReleaseRequest{RequestId: key, Allocation: r.request.Owner.Allocation,
		NodeId: r.h.s.NodeID, OwnerEpoch: r.h.s.Epoch, Mode: pb.ReleaseMode_RELEASE_MODE_NODE, CleanupReceiptId: "verified"})
	return err
}

func TestPublishedBlockRenewReplayRestartAndCleanup(t *testing.T) {
	r := newBlockRenewFixture(t)
	legacy := r.state(t)
	require.NotContains(t, legacy.Data, blockRenewalKey)
	l, err := r.client.Renew(t.Context(), r.request)
	require.NoError(t, err)
	until, err := strconv.ParseInt(legacy.Data["deadline"], 10, 64)
	require.NoError(t, err)
	require.Greater(t, l.RenewByUnixSeconds, until)
	saved := r.state(t)
	require.Equal(t, strconv.FormatInt(l.RenewByUnixSeconds, 10), saved.Data["deadline"])
	require.Equal(t, legacy.Data["legacy-extension"], saved.Data["legacy-extension"])
	ledger, err := blockRenewals(saved)
	require.NoError(t, err)
	require.Len(t, ledger.Requests, 1)
	require.Equal(t, l.RenewByUnixSeconds, ledger.Requests[blockRequestDigest(r.request.RequestId)].Deadline)
	require.Error(t, r.h.EnterTeardown(t.Context()))
	r.restart(t)
	// Cross a clock tick: a replay must not silently add another second.
	time.Sleep(1100 * time.Millisecond)
	replay, err := r.client.Renew(t.Context(), r.request)
	require.NoError(t, err)
	require.True(t, proto.Equal(l, replay))
	require.Equal(t, saved, r.state(t))
	next := proto.CloneOf(r.request)
	next.RequestId = "renew-next"
	renewed, err := r.client.Renew(t.Context(), next)
	require.NoError(t, err)
	require.Greater(t, renewed.RenewByUnixSeconds, replay.RenewByUnixSeconds)
	saved = r.state(t)
	replay, err = r.client.Renew(t.Context(), r.request)
	require.NoError(t, err)
	require.True(t, proto.Equal(renewed, replay), "old key returns current owner snapshot without extending")
	require.Equal(t, saved, r.state(t))
	require.Equal(t, codes.AlreadyExists, status.Code(r.release(t, r.request.RequestId)))
	_, err = r.client.Acquire(t.Context(), &pb.AcquireRequest{RequestId: r.request.RequestId, Owner: r.request.Owner})
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.NoError(t, r.release(t, "release"))
	saved = r.state(t)
	_, err = r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	r.request.RequestId = "release"
	_, err = r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, saved, r.state(t))
	require.NoError(t, r.h.EnterTeardown(t.Context()))
}

func TestPublishedBlockRenewRejectedKeysDoNotGrowHistory(t *testing.T) {
	r := newBlockRenewFixture(t)
	before := r.state(t)
	for i := 0; i < 32; i++ {
		bad := proto.CloneOf(r.request)
		bad.RequestId = fmt.Sprintf("rejected-%d", i)
		bad.Owner.OwnerEpoch++
		_, err := r.client.Renew(t.Context(), bad)
		require.Equal(t, codes.Aborted, status.Code(err))
	}
	for _, change := range []func(*pb.RenewRequest){
		func(q *pb.RenewRequest) { q.RequestId = "" },
		func(q *pb.RenewRequest) { q.RequestId = strings.Repeat("x", maxBlockRequestBytes+1) },
		func(q *pb.RenewRequest) { q.Owner = nil },
		func(q *pb.RenewRequest) { q.Owner.Allocation = nil },
		func(q *pb.RenewRequest) { q.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(q *pb.RenewRequest) { q.Owner.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(q *pb.RenewRequest) { q.Owner.Allocation.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
		func(q *pb.RenewRequest) { q.Owner.Allocation.Key.ProtoReflect().SetUnknown([]byte{0x78, 1}) },
	} {
		bad := proto.CloneOf(r.request)
		change(bad)
		_, err := r.client.Renew(t.Context(), bad)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	}
	for _, role := range []ReservationRole{ReservationReader, ReservationController} {
		_, err := r.connect(t, role, "").Renew(t.Context(), r.request)
		require.Equal(t, codes.PermissionDenied, status.Code(err))
	}
	_, err := r.connect(t, ReservationNode, "other-node").Renew(t.Context(), r.request)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	acquire := proto.CloneOf(r.request)
	acquire.RequestId = "acquire"
	_, err = r.client.Renew(t.Context(), acquire)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	require.Equal(t, before, r.state(t))
	_, err = r.client.Renew(t.Context(), r.request)
	require.NoError(t, err)
	before = r.state(t)
	for _, change := range []func(*pb.RenewRequest){
		func(q *pb.RenewRequest) { q.Owner.OwnerEpoch++ },
		func(q *pb.RenewRequest) { q.Owner.Allocation.DeviceGeneration++ },
		func(q *pb.RenewRequest) { q.Owner.Allocation.ResourceId += "-other" },
		func(q *pb.RenewRequest) { q.Owner.Allocation.StableDeviceId += "-other" },
		func(q *pb.RenewRequest) { q.Owner.Allocation.Key.LeaseId += "-other" },
		func(q *pb.RenewRequest) { q.Owner.Allocation.Key.Provider += "-other" },
	} {
		bad := proto.CloneOf(r.request)
		change(bad)
		_, err := r.client.Renew(t.Context(), bad)
		require.Equal(t, codes.AlreadyExists, status.Code(err))
	}
	require.Equal(t, before, r.state(t))
}

func TestPublishedBlockRenewRequiresFreshAttachment(t *testing.T) {
	for _, failure := range []string{"disk", "vm", "guid", "shared", "provisioning"} {
		t.Run(failure, func(t *testing.T) {
			r := newBlockRenewFixture(t)
			_, err := r.client.Renew(t.Context(), r.request)
			require.NoError(t, err)
			before := r.state(t)
			switch failure {
			case "disk":
				r.d.ManagedBy = ptr.To("another-vm")
			case "vm":
				r.vm.InstanceGUID += "-replacement"
			case "guid":
				r.d.Properties.UniqueID = ptr.To("another-disk")
			case "shared":
				r.d.Properties.MaxShares = ptr.To[int32](2)
			case "provisioning":
				r.d.Properties.ProvisioningState = ptr.To("Updating")
			}
			for _, key := range []string{r.request.RequestId, "new-renew"} {
				q := proto.CloneOf(r.request)
				q.RequestId = key
				result, err := r.client.Renew(t.Context(), q)
				require.Error(t, err)
				require.Nil(t, result)
			}
			require.Equal(t, before, r.state(t))
			require.Error(t, r.h.EnterTeardown(t.Context()))
		})
	}
}

func TestPublishedBlockRenewExpiredNeverRevives(t *testing.T) {
	for _, replay := range []bool{false, true} {
		t.Run(fmt.Sprint(replay), func(t *testing.T) {
			r := newBlockRenewFixture(t)
			if replay {
				_, err := r.client.Renew(t.Context(), r.request)
				require.NoError(t, err)
			}
			// Simulate provider time passing without sleeping for the full TTL.
			r.edit(t, func(cm *v1.ConfigMap) {
				until := time.Now().Add(-time.Second).Unix()
				cm.Data["deadline"] = strconv.FormatInt(until, 10)
				if replay {
					l, err := blockRenewals(r.state(t))
					require.NoError(t, err)
					for key, record := range l.Requests {
						record.Deadline = until
						l.Requests[key] = record
					}
					data, err := json.Marshal(l)
					require.NoError(t, err)
					cm.Data[blockRenewalKey] = string(data)
				}
			})
			result, err := r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Nil(t, result)
			require.Equal(t, "expired", r.state(t).Data["phase"])
			require.Error(t, r.h.EnterTeardown(t.Context()))
			// Even a future deadline cannot revive a latched expired hold.
			r.edit(t, func(cm *v1.ConfigMap) { cm.Data["deadline"] = strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10) })
			r.restart(t)
			before := r.state(t)
			_, err = r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Equal(t, before, r.state(t))
			require.Error(t, r.h.EnterTeardown(t.Context()))
			require.NoError(t, r.release(t, "cleanup"))
			require.NoError(t, r.h.EnterTeardown(t.Context()))
		})
	}
}

func TestPublishedBlockRenewCASAndLostReply(t *testing.T) {
	for _, mode := range []string{"conflict", "lost-reply", "confirmed-lost-reply", "dropped-field"} {
		t.Run(mode, func(t *testing.T) {
			r := newBlockRenewFixture(t)
			before := r.state(t)
			intercept := true
			r.f.client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				if !intercept {
					return false, nil, nil
				}
				next := action.(ktesting.UpdateAction).GetObject().(*v1.ConfigMap).DeepCopy()
				if mode == "confirmed-lost-reply" && next.Data["phase"] == "renewing" {
					return false, nil, nil
				}
				intercept = false
				if mode == "conflict" {
					return true, nil, apierrors.NewConflict(action.GetResource().GroupResource(), next.Name, errors.New("CAS lost"))
				}
				version, err := strconv.Atoi(next.ResourceVersion)
				require.NoError(t, err)
				next.ResourceVersion = strconv.Itoa(version + 1)
				require.NoError(t, r.f.client.Tracker().Update(action.GetResource(), next, next.Namespace))
				if mode == "lost-reply" || mode == "confirmed-lost-reply" {
					return true, nil, errUncertain
				}
				delete(next.Data, "legacy-extension")
				return true, next, nil
			})
			result, err := r.client.Renew(t.Context(), r.request)
			require.Nil(t, result)
			if mode == "conflict" {
				require.Equal(t, codes.Aborted, status.Code(err))
				require.Equal(t, before, r.state(t))
			} else {
				require.Equal(t, codes.Unavailable, status.Code(err))
				require.Contains(t, r.state(t).Data, blockRenewalKey)
			}
			r.restart(t)
			saved := r.state(t)
			result, err = r.client.Renew(t.Context(), r.request)
			require.NoError(t, err)
			if mode != "conflict" {
				require.Equal(t, saved.Data["deadline"], r.state(t).Data["deadline"], "lost commit response must replay without extension")
				if mode == "confirmed-lost-reply" {
					require.Equal(t, saved, r.state(t), "confirmed renewal replay must not write")
				} else {
					require.Equal(t, "renewing", saved.Data["phase"])
					require.Equal(t, "held", r.state(t).Data["phase"], "only timely pending renewal may finish confirmation")
				}
			}
			require.Equal(t, strconv.FormatInt(result.RenewByUnixSeconds, 10), r.state(t).Data["deadline"])
			require.Equal(t, before.Data["legacy-extension"], r.state(t).Data["legacy-extension"])
		})
	}
}

func TestPublishedBlockRenewLateCandidateNeverRevives(t *testing.T) {
	for _, mode := range []string{"late-reply", "late-error", "lost-response", "lost-expiry-latch"} {
		t.Run(mode, func(t *testing.T) {
			r := newBlockRenewFixture(t)
			oldDeadline := time.Now().Add(time.Second).Unix()
			r.edit(t, func(cm *v1.ConfigMap) { cm.Data["deadline"] = strconv.FormatInt(oldDeadline, 10) })
			intercept := true
			lostLatch := false
			r.f.client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
				next := action.(ktesting.UpdateAction).GetObject().(*v1.ConfigMap).DeepCopy()
				if mode == "lost-expiry-latch" && !lostLatch && next.Data["phase"] == "expired" {
					lostLatch = true
					return true, nil, errUncertain
				}
				if !intercept {
					return false, nil, nil
				}
				intercept = false
				require.Equal(t, "renewing", next.Data["phase"])
				if mode != "lost-response" {
					time.Sleep(1100 * time.Millisecond)
				}
				version, err := strconv.Atoi(next.ResourceVersion)
				require.NoError(t, err)
				next.ResourceVersion = strconv.Itoa(version + 1)
				require.NoError(t, r.f.client.Tracker().Update(action.GetResource(), next, next.Namespace))
				if mode == "late-error" || mode == "lost-response" {
					return true, nil, errUncertain
				}
				return true, next, nil
			})
			result, err := r.client.Renew(t.Context(), r.request)
			require.Error(t, err)
			require.Nil(t, result)
			if mode == "lost-response" {
				time.Sleep(1100 * time.Millisecond)
			}
			saved := r.state(t)
			newDeadline, err := strconv.ParseInt(saved.Data["deadline"], 10, 64)
			require.NoError(t, err)
			require.Greater(t, newDeadline, oldDeadline, "server persisted candidate's future deadline")
			require.LessOrEqual(t, oldDeadline, time.Now().Unix())
			ledger, err := blockRenewals(saved)
			require.NoError(t, err)
			require.NotNil(t, ledger.Pending, "ambiguous deadline is never silently confirmed")
			r.restart(t)
			result, err = r.client.GetLease(t.Context(), r.request.Owner.Allocation.Key)
			require.True(t, err != nil || result.State == pb.LeaseState_LEASE_STATE_HELD, "pending deadline must not grant an acquired lease")
			result, err = r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Nil(t, result)
			require.Equal(t, "expired", r.state(t).Data["phase"])
			require.Equal(t, saved.Data["deadline"], r.state(t).Data["deadline"])
			require.Error(t, r.h.EnterTeardown(t.Context()))
			require.NoError(t, r.release(t, "cleanup"))
			require.NoError(t, r.h.EnterTeardown(t.Context()))
		})
	}
}

func TestPublishedBlockRenewPendingCleanup(t *testing.T) {
	r := newBlockRenewFixture(t)
	intercept := true
	r.f.client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
		if !intercept {
			return false, nil, nil
		}
		intercept = false
		next := action.(ktesting.UpdateAction).GetObject().(*v1.ConfigMap).DeepCopy()
		require.Equal(t, "renewing", next.Data["phase"])
		version, err := strconv.Atoi(next.ResourceVersion)
		require.NoError(t, err)
		next.ResourceVersion = strconv.Itoa(version + 1)
		require.NoError(t, r.f.client.Tracker().Update(action.GetResource(), next, next.Namespace))
		return true, nil, errUncertain
	})
	_, err := r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	before := r.state(t)
	other := proto.CloneOf(r.request)
	other.RequestId = "another-renew"
	_, err = r.client.Renew(t.Context(), other)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, before, r.state(t), "pending outcome cannot admit more keys")
	require.Error(t, r.h.EnterTeardown(t.Context()))
	r.restart(t)
	require.NoError(t, r.release(t, "cleanup"))
	_, err = r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.NoError(t, r.h.EnterTeardown(t.Context()))
}

func TestPublishedBlockRenewConcurrentRelease(t *testing.T) {
	r := newBlockRenewFixture(t)
	r.observe = func() {
		r.observe = nil
		require.NoError(t, r.h.ReleaseNode(t.Context(), r.h.s.Binding, r.h.s.Owner, "verified"))
	}
	result, err := r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Nil(t, result)
	require.Equal(t, "released", r.state(t).Data["phase"])
	require.NotContains(t, r.state(t).Data, blockRenewalKey)
	require.NoError(t, r.h.EnterTeardown(t.Context()))
}

func TestPublishedBlockRenewConcurrentRequests(t *testing.T) {
	r := newBlockRenewFixture(t)
	arrived, proceed := make(chan struct{}, 2), make(chan struct{})
	r.observe = func() {
		arrived <- struct{}{}
		<-proceed
	}
	results := make(chan error, 2)
	for _, key := range []string{"renew-one", "renew-two"} {
		q := proto.CloneOf(r.request)
		q.RequestId = key
		go func() {
			_, err := r.client.Renew(t.Context(), q)
			results <- err
		}()
	}
	for range 2 {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			close(proceed)
			t.Fatal("renewal did not reach independent observer")
		}
	}
	close(proceed)
	first, second := <-results, <-results
	if first != nil {
		first, second = second, first
	}
	require.NoError(t, first)
	require.Equal(t, codes.Aborted, status.Code(second))
	ledger, err := blockRenewals(r.state(t))
	require.NoError(t, err)
	require.Len(t, ledger.Requests, 1, "CAS loser must not consume a key or extend")
}

func TestPublishedBlockRenewExpiryDuringEvidenceOrCommit(t *testing.T) {
	for _, stage := range []string{"evidence", "commit"} {
		t.Run(stage, func(t *testing.T) {
			r := newBlockRenewFixture(t)
			r.edit(t, func(cm *v1.ConfigMap) {
				cm.Data["deadline"] = strconv.FormatInt(time.Now().Add(time.Second).Unix(), 10)
			})
			if stage == "evidence" {
				r.observe = func() { time.Sleep(1100 * time.Millisecond) }
			} else {
				intercept := true
				r.f.client.PrependReactor("update", "configmaps", func(action ktesting.Action) (bool, runtime.Object, error) {
					if !intercept {
						return false, nil, nil
					}
					intercept = false
					next := action.(ktesting.UpdateAction).GetObject().(*v1.ConfigMap).DeepCopy()
					version, err := strconv.Atoi(next.ResourceVersion)
					require.NoError(t, err)
					next.ResourceVersion = strconv.Itoa(version + 1)
					require.NoError(t, r.f.client.Tracker().Update(action.GetResource(), next, next.Namespace))
					time.Sleep(1100 * time.Millisecond)
					return true, next, nil
				})
			}
			result, err := r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Nil(t, result)
			require.Equal(t, "expired", r.state(t).Data["phase"])
			if stage == "evidence" {
				require.NotContains(t, r.state(t).Data, blockRenewalKey)
			}
			r.restart(t)
			_, err = r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			require.Error(t, r.h.EnterTeardown(t.Context()))
			require.NoError(t, r.release(t, "cleanup"))
		})
	}
}

func TestPublishedBlockRenewBoundedAndIncompatibleHistory(t *testing.T) {
	r := newBlockRenewFixture(t)
	until, err := strconv.ParseInt(r.state(t).Data["deadline"], 10, 64)
	require.NoError(t, err)
	l := blockRenewalLedger{Version: 1, Requests: map[string]blockRenewal{}}
	for i := 0; i < maxBlockRenewals; i++ {
		l.Requests[blockRequestDigest(fmt.Sprint(i))] = blockRenewal{Input: blockRequestDigest("input"), Deadline: until}
	}
	data, err := json.Marshal(l)
	require.NoError(t, err)
	r.edit(t, func(cm *v1.ConfigMap) { cm.Data[blockRenewalKey] = string(data) })
	before := r.state(t)
	_, err = r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.ResourceExhausted, status.Code(err))
	require.Equal(t, before, r.state(t))
	require.NoError(t, r.release(t, "cleanup"), "full history must not prevent cleanup")
	require.NoError(t, r.h.EnterTeardown(t.Context()))

	for _, raw := range []string{`{"version":2,"requests":{}}`, `{"version":1,"requests":{},"future":true}`, ""} {
		t.Run(raw, func(t *testing.T) {
			r := newBlockRenewFixture(t)
			r.edit(t, func(cm *v1.ConfigMap) { cm.Data[blockRenewalKey] = raw })
			before := r.state(t)
			_, err := r.client.Renew(t.Context(), r.request)
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Equal(t, before, r.state(t))
			require.Error(t, r.h.EnterTeardown(t.Context()))
		})
	}
}

func TestPublishedBlockRenewSelectionIncarnationChanged(t *testing.T) {
	r := newBlockRenewFixture(t)
	_, err := r.client.Renew(t.Context(), r.request)
	require.NoError(t, err)
	cm := r.state(t)
	cm.UID = "replacement"
	require.NoError(t, r.f.client.Tracker().Update(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, cm, cm.Namespace))
	_, err = r.client.Renew(t.Context(), r.request)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.Equal(t, cm, r.state(t))
}
