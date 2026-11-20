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
	"errors"
	"net"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

type blockReceipt struct{ s BlockSelection }

type testOpenAuthority func(context.Context, BlockSelection, [sha256.Size]byte, func(OpenedBlock) error) error

func (f testOpenAuthority) WithOpened(ctx context.Context, s BlockSelection, peer [sha256.Size]byte, check func(OpenedBlock) error) error {
	return f(ctx, s, peer, check)
}

func (c blockReceipt) VerifyCleanup(_ context.Context, s BlockSelection, receipt string) error {
	if s != c.s || receipt != "verified" {
		return errors.New("unverified cleanup")
	}
	return nil
}

func TestPublishedBlockWire(t *testing.T) {
	ctx := t.Context()
	f := fixture(t, "pin")
	vm := newModelAttach()
	s := BlockSelection{Provider: "Origin", PoolID: "ordinary", Binding: binding, Owner: owner, PV: "pv", PVUID: "pvuid", PVC: "pvc", PVCUID: "pvcuid", Namespace: "ns", VMID: vm.vm.ID, VMGUID: vm.vm.InstanceGUID}
	data, err := InitialBlockHold(s)
	require.NoError(t, err)
	maps := f.client.CoreV1().ConfigMaps("ns")
	_, err = maps.Create(ctx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "ordinary", UID: "one-use", ResourceVersion: "1"}, Data: data}, metav1.CreateOptions{})
	require.NoError(t, err)
	h, err := NewBlockHold(s, maps, "ordinary", "one-use", blockReceipt{s})
	require.NoError(t, err)
	done, err := h.BeginSetup(ctx, true)
	require.NoError(t, err)
	require.NoError(t, done())
	d := &armcompute.Disk{ID: &s.ResourceID, Properties: &armcompute.DiskProperties{ProvisioningState: ptr.To("Succeeded"), UniqueID: &s.DiskUniqueID, DiskSizeBytes: &s.CapacityBytes, MaxShares: ptr.To[int32](1)}}
	vm.complete(d)
	ca := testCert(t, 1, nil, false)
	roots := x509.NewCertPool()
	roots.AddCert(ca.Leaf)
	clientCert := testCert(t, 2, &ca, false)
	connect := func(hold *BlockHold) pb.DeviceProviderClient {
		grpcServer, err := NewPublishedBlockServer(hold, func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error) { return d, vm.vm, nil }, testCert(t, 3, &ca, true), roots,
			[]ReservationGrant{{sha256.Sum256(clientCert.Certificate[0]), ReservationNode, []string{s.CompositeID}, s.NodeID}})
		require.NoError(t, err)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = grpcServer.Serve(lis) }()
		t.Cleanup(grpcServer.Stop)
		conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "origin.provider.example", Certificates: []tls.Certificate{clientCert}})))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewDeviceProviderClient(conn)
	}
	client := connect(h)
	key := &pb.LeaseKey{Provider: s.Provider, LeaseId: s.AllocationID}
	initial, err := client.GetLease(ctx, key)
	require.NoError(t, err)
	require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, initial.HoldPolicy)
	require.Zero(t, initial.RenewByUnixSeconds)
	req := &pb.AcquireRequest{RequestId: "acquire", Owner: &pb.Owner{Allocation: initial.Allocation, NodeId: "wrong", OwnerEpoch: s.Epoch}}
	_, err = client.Acquire(ctx, req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	req.Owner.NodeId = s.NodeID
	active, err := client.Acquire(ctx, req)
	require.NoError(t, err)
	require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, active.HoldPolicy)
	require.Zero(t, active.RenewByUnixSeconds)
	_, err = client.Renew(ctx, &pb.RenewRequest{RequestId: "renew", Owner: req.Owner})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	wrong := proto.CloneOf(req.Owner)
	wrong.NodeId = "wrong"
	_, err = client.Renew(ctx, &pb.RenewRequest{RequestId: "renew", Owner: wrong})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	staleRenew := proto.CloneOf(req.Owner)
	staleRenew.OwnerEpoch++
	_, err = client.Renew(ctx, &pb.RenewRequest{RequestId: "renew", Owner: staleRenew})
	require.Equal(t, codes.Aborted, status.Code(err))
	again, err := client.Acquire(ctx, req)
	require.NoError(t, err)
	require.True(t, proto.Equal(active, again))
	require.Empty(t, active.ObservedDevicePath)
	validate := &pb.ValidateRequest{Owner: req.Owner, Opened: &pb.OpenedDevice{StableDeviceId: s.DiskUniqueID, DeviceGeneration: s.DeviceGeneration,
		CapacityBytes: uint64(s.CapacityBytes), ObservedMajor: 8, ObservedMinor: 16}}
	_, err = client.Validate(ctx, validate)
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "absent node authority")
	pinned := OpenedBlock{s, 8, 16}
	prove := func(e OpenedBlock) BlockOpenAuthority {
		return testOpenAuthority(func(_ context.Context, selection BlockSelection, peer [sha256.Size]byte, check func(OpenedBlock) error) error {
			if selection != s || peer != sha256.Sum256(clientCert.Certificate[0]) {
				return errors.New("no custody for this peer/selection")
			}
			return check(e)
		})
	}
	client = connect(h.WithOpenedAuthority(prove(pinned)))
	f.client.ClearActions()
	_, err = client.Validate(ctx, validate)
	require.NoError(t, err)
	for _, action := range f.client.Actions() {
		require.Equal(t, "get", action.GetVerb(), "Validate must not update the journal")
	}
	for _, change := range []func(*pb.OpenedDevice){
		func(x *pb.OpenedDevice) { x.StableDeviceId += "x" }, func(x *pb.OpenedDevice) { x.DeviceGeneration++ },
		func(x *pb.OpenedDevice) { x.CapacityBytes++ }, func(x *pb.OpenedDevice) { x.ObservedMajor++ }, func(x *pb.OpenedDevice) { x.ObservedMinor++ },
	} {
		bad := proto.CloneOf(validate)
		change(bad.Opened)
		result, err := client.Validate(ctx, bad)
		require.Equal(t, codes.Aborted, status.Code(err))
		require.Nil(t, result)
	}
	badOwner := proto.CloneOf(validate)
	badOwner.Owner.NodeId = "other"
	_, err = client.Validate(ctx, badOwner)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	stale := pinned
	stale.Selection.Epoch++
	_, err = connect(h.WithOpenedAuthority(prove(stale))).Validate(ctx, validate)
	require.Equal(t, codes.Aborted, status.Code(err))
	d.ManagedBy = ptr.To("another-vm")
	_, err = client.Validate(ctx, validate)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	d.ManagedBy = ptr.To(vm.vm.ID)
	release := &pb.ReleaseRequest{RequestId: "release", Allocation: initial.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_NODE, NodeId: s.NodeID, OwnerEpoch: s.Epoch, CleanupReceiptId: "asserted"}
	_, err = client.Release(ctx, release)
	require.Error(t, err)
	require.Error(t, h.EnterTeardown(ctx))
	release.CleanupReceiptId = "verified"
	// Release after the observations but before custody returns: the final
	// resourceVersion check must not validate the former owner.
	interleaved := testOpenAuthority(func(ctx context.Context, _ BlockSelection, _ [32]byte, check func(OpenedBlock) error) error {
		if err := check(pinned); err != nil {
			return err
		}
		_, err := client.Release(ctx, release)
		return err
	})
	result, err := connect(h.WithOpenedAuthority(interleaved)).Validate(ctx, validate)
	require.Equal(t, codes.Aborted, status.Code(err))
	require.Nil(t, result)
	_, err = client.Validate(ctx, validate)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	_, err = client.Release(ctx, release)
	require.NoError(t, err)
	after, err := client.GetLease(ctx, key)
	require.NoError(t, err)
	require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, after.State)
	require.Equal(t, s.Epoch, after.OwnerEpoch)
	require.NoError(t, h.EnterTeardown(ctx))
}
