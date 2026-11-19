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
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

func testCert(t *testing.T, serial int64, ca *tls.Certificate, server bool) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	x := &x509.Certificate{SerialNumber: big.NewInt(serial), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature}
	parent, signer := x, key
	if ca == nil {
		x.IsCA, x.BasicConstraintsValid = true, true
		x.KeyUsage |= x509.KeyUsageCertSign
	} else {
		parent, signer = ca.Leaf, ca.PrivateKey.(*ecdsa.PrivateKey)
		if server {
			x.DNSNames = []string{"origin.provider.example"}
			x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			x.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, x, parent, &key.PublicKey, signer)
	require.NoError(t, err)
	leaf, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

// Hold the RPC just after the durable operation so the client can lose its
// connection without receiving a reply. This does not fake an RPC result.
type pauseAfterCommit struct {
	ProvisioningScope
	committed, resume chan struct{}
}

func (s pauseAfterCommit) Within(ctx context.Context, uid types.UID, p RawPool, fn func() error) error {
	err := s.ProvisioningScope.Within(ctx, uid, p, fn)
	close(s.committed)
	select {
	case <-s.resume:
	case <-ctx.Done():
	}
	return err
}

func TestReservationWire(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	ca := testCert(t, 1, nil, false)
	roots := x509.NewCertPool()
	roots.AddCert(ca.Leaf)
	serverCert := testCert(t, 2, &ca, true)
	controller, reader, other, unmapped := testCert(t, 3, &ca, false), testCert(t, 4, &ca, false), testCert(t, 5, &ca, false), testCert(t, 6, &ca, false)
	grants := []ReservationGrant{
		{sha256.Sum256(controller.Certificate[0]), ReservationController, []string{"Composite-A"}},
		{sha256.Sum256(reader.Certificate[0]), ReservationReader, []string{"Composite-A"}},
		{sha256.Sum256(other.Certificate[0]), ReservationController, []string{"composite-a"}},
	}
	f := fixture(t, "inventory-guid")
	f.disks.disk = &armcompute.Disk{ID: ptr.To(resourceID), Location: ptr.To(f.pool.Location), Properties: &armcompute.DiskProperties{
		ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateUnattached), UniqueID: ptr.To(f.pool.InventoryGUID),
		DiskSizeBytes: ptr.To(f.pool.Bytes), CreationData: &armcompute.CreationData{CreateOption: ptr.To(armcompute.DiskCreateOptionEmpty)},
	}}
	start := func(r *Reservations) (string, *grpc.Server) {
		t.Helper()
		s, err := NewReservationServer(r, serverCert, roots, grants)
		require.NoError(t, err)
		l, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		go func() { _ = s.Serve(l) }()
		t.Cleanup(s.Stop)
		return l.Addr().String(), s
	}
	dial := func(addr string, c ...tls.Certificate) (pb.DeviceProviderClient, *grpc.ClientConn) {
		t.Helper()
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13,
			ServerName: "origin.provider.example", RootCAs: roots, Certificates: c})))
		require.NoError(t, err)
		t.Cleanup(func() { _ = conn.Close() })
		return pb.NewDeviceProviderClient(conn), conn
	}
	pool := &pb.Pool{Provider: f.pool.Provider, PoolId: f.pool.PoolID}
	req := &pb.ReserveRequest{RequestId: "request-1", Pool: pool, CompositeVolumeId: "Composite-A", CapacityBytes: 512}
	r := f.adapter(t)
	pause := pauseAfterCommit{r.scope, make(chan struct{}), make(chan struct{})}
	r.scope = pause
	addr, s := start(r)
	client, conn := dial(addr, controller)
	lost := make(chan error, 1)
	go func() { _, err := client.Reserve(ctx, req); lost <- err }()
	select {
	case <-pause.committed:
	case <-ctx.Done():
		t.Fatal("reservation did not reach commit")
	}
	require.NoError(t, conn.Close())
	require.Error(t, <-lost)
	close(pause.resume)
	s.Stop()

	addr, _ = start(f.adapter(t)) // same API object and disk, new server and adapter
	client, _ = dial(addr, controller)
	f.client.ClearActions()
	b, err := client.Reserve(ctx, req)
	require.NoError(t, err)
	expected := &pb.Lease{Allocation: &pb.Allocation{Key: &pb.LeaseKey{Provider: pool.Provider, LeaseId: r.allocationID()},
		CompositeVolumeId: "Composite-A", ResourceId: resourceID, StableDeviceId: f.pool.InventoryGUID, DeviceGeneration: 1},
		PoolId: pool.PoolId, CapacityBytes: uint64(f.pool.Bytes), State: pb.LeaseState_LEASE_STATE_RESERVED}
	require.True(t, proto.Equal(expected, b), "unexpected RESERVED snapshot: %v", b)
	for _, a := range f.client.Actions() {
		require.Equal(t, "get", a.GetVerb(), "replay mutated the journal")
	}
	changed := proto.CloneOf(req)
	changed.Pool.PoolId = "Pool"
	_, err = client.Reserve(ctx, changed)
	require.Equal(t, codes.AlreadyExists, status.Code(err))
	read, _ := dial(addr, reader)
	seen, err := read.GetLease(ctx, b.Allocation.Key)
	require.NoError(t, err)
	require.True(t, proto.Equal(b, seen))
	cap, err := read.GetCapacity(ctx, pool)
	require.NoError(t, err)
	require.Zero(t, cap.AvailableBytes)
	require.Zero(t, cap.MaxAllocationBytes)
	_, err = read.Reserve(ctx, req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	foreign, _ := dial(addr, other)
	_, err = foreign.Reserve(ctx, req)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	seen, err = foreign.GetLease(ctx, b.Allocation.Key)
	require.Nil(t, seen)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	for _, bad := range []*pb.Pool{{Provider: "Provider", PoolId: "pool"}, {Provider: "provider", PoolId: "Pool"}} {
		_, err = client.GetCapacity(ctx, bad)
		require.Equal(t, codes.NotFound, status.Code(err))
	}
	unknown, _ := dial(addr, unmapped)
	_, err = unknown.GetCapacity(ctx, pool)
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	anonymous, _ := dial(addr)
	_, err = anonymous.GetCapacity(ctx, pool)
	require.Error(t, err)
	_, err = client.Acquire(ctx, &pb.AcquireRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	_, err = client.Release(ctx, &pb.ReleaseRequest{})
	require.Equal(t, codes.Unimplemented, status.Code(err))
	require.Zero(t, f.disks.creates)
}

func TestReservationWireRejectsDynamicSlot(t *testing.T) {
	_, err := NewReservationServer(fixture(t, "").adapter(t), tls.Certificate{}, nil, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}
