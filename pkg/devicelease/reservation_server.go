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
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

type ReservationRole string

const (
	ReservationReader     ReservationRole = "reader"
	ReservationController ReservationRole = "controller"
	ReservationNode       ReservationRole = "node"
)

// ReservationGrant identifies one verified leaf certificate by its DER
// digest. Composite IDs are exact matches, not patterns or caller metadata.
type ReservationGrant struct {
	ClientSHA256 [sha256.Size]byte
	Role         ReservationRole
	CompositeIDs []string
	NodeID       string // Required only for an acquiring node's certificate.
}

type reservationGrant struct {
	role       ReservationRole
	volumes    map[string]bool
	node       string
	peerDigest [sha256.Size]byte
}
type grantContextKey struct{}

type reservationRPC struct {
	pb.UnimplementedDeviceProviderServer
	r      *Reservations
	block  *publishedRPC
	grants map[[sha256.Size]byte]reservationGrant
}

// NewReservationServer constructs but does not listen. Register only on a
// dedicated mTLS transport, never on the ordinary CSI socket. The wire path
// cannot create a disk: even an authorized controller needs pinned inventory.
func NewReservationServer(r *Reservations, cert tls.Certificate, clientCAs *x509.CertPool, grants []ReservationGrant) (*grpc.Server, error) {
	if r == nil || r.scope == nil || r.maps == nil || r.disks == nil || !r.pool.valid() || r.pool.InventoryGUID == "" {
		return nil, status.Error(codes.FailedPrecondition, "pre-enrolled origin inventory and independent provisioning authority required")
	}
	return (&reservationRPC{r: r}).server(cert, clientCAs, grants)
}

func (s *reservationRPC) server(cert tls.Certificate, clientCAs *x509.CertPool, grants []ReservationGrant) (*grpc.Server, error) {
	if len(cert.Certificate) == 0 || cert.PrivateKey == nil || clientCAs == nil || len(grants) == 0 {
		return nil, status.Error(codes.InvalidArgument, "server keypair, explicit client roots and client grants required")
	}
	s.grants = make(map[[sha256.Size]byte]reservationGrant, len(grants))
	for _, g := range grants {
		if _, duplicate := s.grants[g.ClientSHA256]; duplicate || g.ClientSHA256 == ([sha256.Size]byte{}) || (g.Role != ReservationReader && g.Role != ReservationController && g.Role != ReservationNode) || len(g.CompositeIDs) == 0 ||
			(g.Role == ReservationNode && !validID(g.NodeID)) || (g.Role != ReservationNode && g.NodeID != "") {
			return nil, status.Error(codes.InvalidArgument, "invalid or duplicate client grant")
		}
		entry := reservationGrant{g.Role, make(map[string]bool, len(g.CompositeIDs)), g.NodeID, g.ClientSHA256}
		for _, id := range g.CompositeIDs {
			if !validID(id) {
				return nil, status.Error(codes.InvalidArgument, "empty or malformed composite scope")
			}
			entry.volumes[id] = true
		}
		s.grants[g.ClientSHA256] = entry
	}
	t := &tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientCAs: clientCAs.Clone(), ClientAuth: tls.RequireAndVerifyClientCert, SessionTicketsDisabled: true}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(t)), grpc.UnaryInterceptor(s.authorize))
	pb.RegisterDeviceProviderServer(server, s)
	return server, nil
}

func (s *reservationRPC) authorize(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
	}
	chain := ti.State.VerifiedChains[0]
	now := time.Now()
	for _, cert := range chain {
		if now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return nil, status.Error(codes.Unauthenticated, "client certificate chain expired or not yet valid")
		}
	}
	g, ok := s.grants[sha256.Sum256(chain[0].Raw)]
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "no origin client grant")
	}
	switch info.FullMethod {
	case pb.DeviceProvider_GetLease_FullMethodName:
	case pb.DeviceProvider_GetCapacity_FullMethodName:
		if s.block != nil {
			return nil, status.Error(codes.Unimplemented, "selected block PV is already provisioned")
		}
	case pb.DeviceProvider_Release_FullMethodName, pb.DeviceProvider_Validate_FullMethodName, pb.DeviceProvider_Renew_FullMethodName:
		if s.block == nil {
			return nil, status.Error(codes.Unimplemented, "published block operation unavailable")
		}
		if g.role != ReservationNode {
			return nil, status.Error(codes.PermissionDenied, "node-specific grant required")
		}
	case pb.DeviceProvider_Acquire_FullMethodName:
		if s.block == nil && s.r.attach == nil {
			return nil, status.Error(codes.Unimplemented, "controller acquisition disabled")
		}
		if g.role != ReservationNode {
			return nil, status.Error(codes.PermissionDenied, "node-specific grant required")
		}
	case pb.DeviceProvider_Reserve_FullMethodName:
		if s.block != nil {
			return nil, status.Error(codes.Unimplemented, "selected block PV is already provisioned")
		}
		if g.role != ReservationController {
			return nil, status.Error(codes.PermissionDenied, "controller grant required")
		}
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported origin operation")
	}
	return next(context.WithValue(ctx, grantContextKey{}, g), req)
}

func (s *reservationRPC) checkProvider(p *pb.Pool) error {
	if p == nil || !validID(p.Provider) || !validID(p.PoolId) || len(p.ProtoReflect().GetUnknown()) != 0 {
		return status.Error(codes.InvalidArgument, "invalid origin pool")
	}
	if p.Provider != s.r.pool.Provider {
		return status.Error(codes.NotFound, "unknown origin pool")
	}
	return nil
}

func (s *reservationRPC) GetCapacity(ctx context.Context, p *pb.Pool) (*pb.Capacity, error) {
	if err := s.checkProvider(p); err != nil {
		return nil, err
	}
	n, err := s.r.Capacity(ctx, p.PoolId)
	if err != nil {
		return nil, err
	}
	return &pb.Capacity{AvailableBytes: n, MaxAllocationBytes: n}, nil
}

func (s *reservationRPC) Reserve(ctx context.Context, in *pb.ReserveRequest) (*pb.Lease, error) {
	if in == nil || !validID(in.RequestId) || !validID(in.CompositeVolumeId) || in.CapacityBytes == 0 || len(in.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid origin reservation")
	}
	// Reserve checks the exact pool in the journal so a replay that changes
	// only the pool still receives ALREADY_EXISTS for its admitted key.
	if err := s.checkProvider(in.Pool); err != nil {
		return nil, err
	}
	if !ctx.Value(grantContextKey{}).(reservationGrant).volumes[in.CompositeVolumeId] {
		return nil, status.Error(codes.PermissionDenied, "composite outside client grant")
	}
	b, err := s.r.reserve(ctx, in.RequestId, in.Pool.PoolId, in.CompositeVolumeId, in.CapacityBytes)
	if err != nil {
		return nil, err
	}
	return s.snapshot(b), nil
}

func (s *reservationRPC) Acquire(ctx context.Context, in *pb.AcquireRequest) (*pb.Lease, error) {
	if s.block != nil {
		return s.block.acquire(ctx, in)
	}
	o := in.GetOwner()
	a := o.GetAllocation()
	k := a.GetKey()
	if k == nil || !validID(k.Provider) || len(in.ProtoReflect().GetUnknown()) != 0 || len(o.ProtoReflect().GetUnknown()) != 0 || len(a.ProtoReflect().GetUnknown()) != 0 || len(k.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid acquire owner")
	}
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	if !g.volumes[a.CompositeVolumeId] || g.node != o.NodeId {
		return nil, status.Error(codes.PermissionDenied, "node or composite outside client grant")
	}
	if k.Provider != s.r.pool.Provider {
		return nil, status.Error(codes.NotFound, "unknown origin provider")
	}
	v, err := s.r.acquire(ctx, in.RequestId, attachInput{Binding: Binding{AllocationID: k.LeaseId, CompositeID: a.CompositeVolumeId, ResourceID: a.ResourceId,
		DiskUniqueID: a.StableDeviceId, DeviceGeneration: a.DeviceGeneration, CapacityBytes: s.r.pool.Bytes}, Owner: Owner{NodeID: o.NodeId, Epoch: o.OwnerEpoch}, Previous: in.PreviousOwnerEpoch})
	if err != nil {
		return nil, err
	}
	return s.snapshot(v), nil
}

func (s *reservationRPC) GetLease(ctx context.Context, k *pb.LeaseKey) (*pb.Lease, error) {
	if s.block != nil {
		return s.block.lookup(ctx, k)
	}
	if k == nil || !validID(k.Provider) || !validID(k.LeaseId) || len(k.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid lease key")
	}
	if k.Provider != s.r.pool.Provider {
		return nil, status.Error(codes.NotFound, "unknown origin provider")
	}
	granted := ctx.Value(grantContextKey{}).(reservationGrant).volumes
	b, err := s.r.lookup(ctx, k.LeaseId, func(composite string) bool { return granted[composite] })
	if err != nil {
		return nil, err
	}
	return s.snapshot(b), nil
}

func (s *reservationRPC) snapshot(b ownerView) *pb.Lease {
	state := pb.LeaseState_LEASE_STATE_RESERVED
	if b.Epoch != 0 {
		state = pb.LeaseState_LEASE_STATE_ACQUIRED
	}
	if b.Held {
		state = pb.LeaseState_LEASE_STATE_HELD
	}
	return &pb.Lease{Allocation: &pb.Allocation{Key: &pb.LeaseKey{Provider: s.r.pool.Provider, LeaseId: b.AllocationID}, CompositeVolumeId: b.CompositeID,
		ResourceId: b.ResourceID, StableDeviceId: b.DiskUniqueID, DeviceGeneration: b.DeviceGeneration}, PoolId: s.r.pool.PoolID,
		CapacityBytes: uint64(b.CapacityBytes), State: state, NodeId: b.NodeID, OwnerEpoch: b.Epoch, RenewByUnixSeconds: b.Until}
}
