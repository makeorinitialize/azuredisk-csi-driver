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
	"testing"
	"testing/synctest"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

func explicitFixture(t *testing.T) (*onlineFixture, *onlineRPC, context.Context, *pb.AcquireRequest) {
	t.Helper()
	f := newOnlineFixture(t)
	f.attach()
	observe := func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error) {
		return f.disk, f.vm, nil
	}
	l, err := f.d.reserve(t.Context(), f.req, observe)
	require.NoError(t, err)
	require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, l.HoldPolicy)
	require.Zero(t, l.RenewByUnixSeconds)
	ctx := context.WithValue(t.Context(), grantContextKey{}, reservationGrant{role: ReservationNode, node: "vm1"})
	return f, &onlineRPC{domain: f.d, observe: observe}, ctx,
		&pb.AcquireRequest{RequestId: "acquire", Owner: &pb.Owner{Allocation: l.Allocation, NodeId: "vm1", OwnerEpoch: 1}}
}

func TestExplicitHoldSurvivesCenturiesAndProcessReconstruction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f, rpc, ctx, acquire := explicitFixture(t)
		held, err := rpc.Acquire(ctx, acquire)
		require.NoError(t, err)
		before, _, err := f.d.read(ctx)
		require.NoError(t, err)
		for i := 0; i < 3; i++ {
			// Fake time advances without a real sleep; no lease clock or
			// periodic supervisor exists to keep this authorization alive.
			time.Sleep(100 * 365 * 24 * time.Hour)
			f.restart(t)
			rpc.domain = f.d
			replay, err := rpc.Acquire(ctx, acquire)
			require.NoError(t, err)
			require.True(t, proto.Equal(held, replay))
			require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, replay.HoldPolicy)
			require.Zero(t, replay.RenewByUnixSeconds)
			current, err := rpc.GetLease(ctx, held.Allocation.Key)
			require.NoError(t, err)
			require.True(t, proto.Equal(held, current))
			successor := proto.CloneOf(acquire)
			successor.RequestId, successor.Owner.OwnerEpoch = "successor", 2
			_, err = rpc.Acquire(ctx, successor)
			require.Equal(t, codes.Aborted, status.Code(err))
			_, _, err = f.d.BeginNative(ctx, "ControllerUnpublishVolume", "detach", resourceID, "", "")
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = rpc.Release(ctx, &pb.ReleaseRequest{RequestId: "close", Allocation: held.Allocation,
				OwnerEpoch: 1, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			_, err = rpc.Renew(ctx, &pb.RenewRequest{RequestId: "renew", Owner: acquire.Owner})
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
		}
		after, r, err := f.d.read(ctx)
		require.NoError(t, err)
		require.Equal(t, before, after, "elapsed time and unsupported renewal must not write or revoke")
		h := f.d.hold(r)
		require.Error(t, h.ReleaseNode(ctx, h.s.Binding, h.s.Owner, "verified"), "no installed authority")
		h.cleanup = blockReceipt{h.s}
		require.Error(t, h.ReleaseNode(ctx, h.s.Binding, h.s.Owner, "asserted"), "assertions do not release")
		require.NoError(t, h.ReleaseNode(ctx, h.s.Binding, h.s.Owner, "verified"))
		native, _, err := f.d.BeginNative(ctx, "ControllerUnpublishVolume", "detach", resourceID, "", "")
		require.NoError(t, err)
		require.NoError(t, f.d.EndNative(native, true, "", nil))
		released, err := rpc.GetLease(ctx, held.Allocation.Key)
		require.NoError(t, err)
		require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, released.State)
		require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, released.HoldPolicy)
		require.Zero(t, released.RenewByUnixSeconds)
		require.Equal(t, uint64(1), released.OwnerEpoch)
		_, err = rpc.Acquire(ctx, acquire)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestExplicitHoldQuarantinesLegacyStateWithoutReleaseOrReuse(t *testing.T) {
	for _, phase := range []string{"selected", "held", "renewing", "expired"} {
		for _, kind := range []string{"old-version", "missing-policy", "unknown-policy", "old-deadline", "future-deadline", "renewal-history"} {
			t.Run(phase+"/"+kind, func(t *testing.T) {
				f, rpc, ctx, acquire := explicitFixture(t)
				if phase != "selected" {
					_, err := rpc.Acquire(ctx, acquire)
					require.NoError(t, err)
				}
				cm, r, err := f.d.read(ctx)
				require.NoError(t, err)
				cm.Data["phase"] = phase
				switch kind {
				case "old-version":
					r.Version = 1
					b, err := json.Marshal(r)
					require.NoError(t, err)
					cm.Data[onlineDomainKey] = string(b)
				case "missing-policy":
					delete(cm.Data, "hold-policy.v1")
				case "unknown-policy":
					cm.Data["hold-policy.v1"] = "unspecified"
				case "old-deadline":
					cm.Data["deadline"] = "1"
				case "future-deadline":
					cm.Data["deadline"] = "9223372036854775807"
				case "renewal-history":
					cm.Data["renewals.v1"] = `{"version":1,"requests":{}}`
				}
				before, err := f.d.maps.Update(ctx, cm, metav1.UpdateOptions{})
				require.NoError(t, err)
				f.restart(t)
				rpc.domain = f.d
				require.Equal(t, codes.Unavailable, status.Code(f.d.Check(ctx)))
				_, err = rpc.GetLease(ctx, acquire.Owner.Allocation.Key)
				require.Equal(t, codes.Unavailable, status.Code(err))
				_, err = rpc.Acquire(ctx, acquire)
				require.Equal(t, codes.Unavailable, status.Code(err))
				_, err = rpc.Reserve(ctx, f.req)
				require.Equal(t, codes.Unavailable, status.Code(err))
				_, err = rpc.Release(ctx, &pb.ReleaseRequest{RequestId: "close", Allocation: acquire.Owner.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION})
				require.Equal(t, codes.Unavailable, status.Code(err))
				h := f.d.hold(r)
				h.cleanup = blockReceipt{h.s}
				require.Equal(t, codes.Unavailable, status.Code(h.ReleaseNode(ctx, h.s.Binding, h.s.Owner, "verified")))
				for _, method := range []string{"ControllerPublishVolume", "ControllerUnpublishVolume", "DeleteVolume"} {
					_, _, err = f.d.BeginNative(ctx, method, "native", resourceID, "", "")
					require.Equal(t, codes.Unavailable, status.Code(err))
				}
				after, err := f.d.maps.Get(ctx, f.d.name, metav1.GetOptions{})
				require.NoError(t, err)
				require.Equal(t, before, after)
			})
		}
	}
}

func TestExplicitAcquireLostReplyRetainsExactOwner(t *testing.T) {
	f, rpc, ctx, acquire := explicitFixture(t)
	base := f.d.maps
	writes := 0
	f.d.maps = &completionMaps{ConfigMapInterface: base, update: func(ctx context.Context, cm *core.ConfigMap, opts metav1.UpdateOptions) (*core.ConfigMap, error) {
		writes++
		_, err := base.Update(ctx, cm, opts)
		require.NoError(t, err)
		return nil, errUncertain
	}}
	l, err := rpc.Acquire(ctx, acquire)
	require.Nil(t, l)
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.d.maps = base
	f.restart(t)
	rpc.domain = f.d
	replay, err := rpc.Acquire(ctx, acquire)
	require.NoError(t, err)
	require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, replay.HoldPolicy)
	require.Zero(t, replay.RenewByUnixSeconds)
	require.Equal(t, 1, writes)
	successor := proto.CloneOf(acquire)
	successor.RequestId, successor.Owner.OwnerEpoch = "successor", 2
	_, err = rpc.Acquire(ctx, successor)
	require.Equal(t, codes.Aborted, status.Code(err))
	_, _, err = f.d.BeginNative(ctx, "ControllerUnpublishVolume", "detach", resourceID, "", "")
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
