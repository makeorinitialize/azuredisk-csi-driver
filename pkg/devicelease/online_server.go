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
	"encoding/hex"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

// OnlineGrant authorizes exactly the configured, independently resolved pool,
// not arbitrary composite names or namespaces supplied by the caller.
type OnlineGrant struct {
	ClientSHA256 string          `json:"clientSHA256"`
	Role         ReservationRole `json:"role"`
	NodeID       string          `json:"nodeID,omitempty"`
}

type onlineRPC struct {
	pb.UnimplementedDeviceProviderServer
	domain  *OnlineDomain
	observe BlockObserver
	grants  map[[sha256.Size]byte]reservationGrant
}

func NewOnlineServer(d *OnlineDomain, observe BlockObserver, cert tls.Certificate, roots *x509.CertPool, grants []OnlineGrant) (*grpc.Server, error) {
	if d == nil || observe == nil || len(cert.Certificate) == 0 || cert.PrivateKey == nil || roots == nil || len(grants) == 0 {
		return nil, status.Error(codes.InvalidArgument, "online domain, observer, server keypair, roots and grants required")
	}
	s := &onlineRPC{domain: d, observe: observe, grants: map[[sha256.Size]byte]reservationGrant{}}
	for _, grant := range grants {
		raw, err := hex.DecodeString(grant.ClientSHA256)
		if err != nil || len(raw) != sha256.Size ||
			grant.Role != ReservationReader && grant.Role != ReservationController && grant.Role != ReservationNode ||
			grant.Role == ReservationNode && grant.NodeID != d.identity.Backing.NodeName ||
			grant.Role != ReservationNode && grant.NodeID != "" {
			return nil, status.Error(codes.InvalidArgument, "invalid online pool grant")
		}
		var digest [sha256.Size]byte
		copy(digest[:], raw)
		if _, exists := s.grants[digest]; exists {
			return nil, status.Error(codes.InvalidArgument, "duplicate online certificate grant")
		}
		s.grants[digest] = reservationGrant{role: grant.Role, node: grant.NodeID, peerDigest: digest}
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{cert},
		ClientCAs: roots.Clone(), ClientAuth: tls.RequireAndVerifyClientCert, SessionTicketsDisabled: true})), grpc.UnaryInterceptor(s.authorize))
	pb.RegisterDeviceProviderServer(server, s)
	return server, nil
}

func (s *onlineRPC) authorize(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
	}
	ti, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(ti.State.VerifiedChains) == 0 || len(ti.State.VerifiedChains[0]) == 0 {
		return nil, status.Error(codes.Unauthenticated, "verified client certificate required")
	}
	now := time.Now()
	for _, cert := range ti.State.VerifiedChains[0] {
		if now.Before(cert.NotBefore) || !now.Before(cert.NotAfter) {
			return nil, status.Error(codes.Unauthenticated, "client certificate expired or not yet valid")
		}
	}
	g, ok := s.grants[sha256.Sum256(ti.State.VerifiedChains[0][0].Raw)]
	if !ok {
		return nil, status.Error(codes.PermissionDenied, "no online pool grant")
	}
	role := ReservationReader
	switch info.FullMethod {
	case pb.DeviceProvider_GetLease_FullMethodName, pb.DeviceProvider_GetCapacity_FullMethodName:
	case pb.DeviceProvider_Reserve_FullMethodName:
		role = ReservationController
	case pb.DeviceProvider_Acquire_FullMethodName, pb.DeviceProvider_Validate_FullMethodName, pb.DeviceProvider_Renew_FullMethodName:
		role = ReservationNode
	case pb.DeviceProvider_Release_FullMethodName:
		r, ok := req.(*pb.ReleaseRequest)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid release")
		}
		if r.Mode == pb.ReleaseMode_RELEASE_MODE_ALLOCATION {
			role = ReservationController
		} else {
			role = ReservationNode
		}
	default:
		return nil, status.Error(codes.Unimplemented, "unsupported online operation")
	}
	if role != ReservationReader && g.role != role {
		return nil, status.Error(codes.PermissionDenied, "operation outside online pool grant")
	}
	return next(context.WithValue(ctx, grantContextKey{}, g), req)
}

func (s *onlineRPC) GetCapacity(context.Context, *pb.Pool) (*pb.Capacity, error) {
	return nil, status.Error(codes.Unimplemented, "external source holds do not provision capacity")
}

func (s *onlineRPC) Reserve(ctx context.Context, in *pb.ReserveRequest) (*pb.Lease, error) {
	l, err := s.domain.reserve(ctx, in, s.observe)
	if err != nil {
		return nil, err
	}
	return s.GetLease(ctx, l.Allocation.Key)
}

func (s *onlineRPC) route(ctx context.Context, key *pb.LeaseKey) (context.Context, *reservationRPC, onlineRecord, error) {
	_, r, err := s.domain.read(ctx)
	if err != nil {
		return ctx, nil, r, err
	}
	if r.Closed && r.Selection != nil && proto.Equal(key, s.domain.reservedLease(r).Allocation.Key) {
		return ctx, nil, r, status.Error(codes.NotFound, "online allocation permanently closed")
	}
	if r.Busy != nil && (r.Selection == nil || nativeTouches(r, r.Busy.Method, r.Busy.ResourceID)) {
		return ctx, nil, r, status.Error(codes.Unavailable, "native admission retained")
	}
	if r.Selection == nil || key == nil || !proto.Equal(key, s.domain.reservedLease(r).Allocation.Key) || r.Closed {
		return ctx, nil, r, status.Error(codes.NotFound, "online allocation absent or permanently closed")
	}
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	g.volumes = map[string]bool{r.CompositeID: true}
	ctx = context.WithValue(ctx, grantContextKey{}, g)
	return ctx, &reservationRPC{block: &publishedRPC{s.domain.hold(r), s.observe}}, r, nil
}

func (s *onlineRPC) GetLease(ctx context.Context, key *pb.LeaseKey) (*pb.Lease, error) {
	ctx, rpc, r, err := s.route(ctx, key)
	if err != nil {
		return nil, err
	}
	cm, err := rpc.block.h.read(ctx)
	if err != nil {
		return nil, err
	}
	if cm.Data["phase"] == "selected" {
		return s.domain.reservedLease(r), nil
	}
	return rpc.GetLease(ctx, key)
}

func (s *onlineRPC) ownerRoute(ctx context.Context, owner *pb.Owner, requestID string, admission bool) (context.Context, *reservationRPC, error) {
	ctx, rpc, r, err := s.route(ctx, owner.GetAllocation().GetKey())
	if err != nil {
		return ctx, nil, err
	}
	if requestID != "" && (requestID == r.ReserveID || requestID == r.ReleaseID) {
		return ctx, nil, status.Error(codes.AlreadyExists, "request key belongs to another operation")
	}
	if err := rpc.block.checkOwner(ctx, owner.GetAllocation(), owner.GetNodeId(), owner.GetOwnerEpoch()); err != nil {
		return ctx, nil, err
	}
	if admission {
		cm, err := rpc.block.h.read(ctx)
		if err != nil {
			return ctx, nil, err
		}
		// Once an owner exists, loss/deletion of source objects cannot make
		// that owner disappear. Replays use retained identity and fresh RPs.
		if cm.Data["acquireID"] == "" {
			proof, err := s.domain.resolveBacking(ctx, r.CompositeID)
			if err != nil {
				return ctx, nil, err
			}
			if r.Backing == nil || !proof.Journal.same(*r.Backing) || !strings.EqualFold(proof.VMID, r.Selection.VMID) {
				return ctx, nil, backingIdentityError()
			}
		}
	}
	return ctx, rpc, nil
}

func (s *onlineRPC) Acquire(ctx context.Context, in *pb.AcquireRequest) (*pb.Lease, error) {
	ctx, rpc, err := s.ownerRoute(ctx, in.GetOwner(), in.GetRequestId(), true)
	if err != nil {
		return nil, err
	}
	return rpc.Acquire(ctx, in)
}

func (s *onlineRPC) Renew(ctx context.Context, in *pb.RenewRequest) (*pb.Lease, error) {
	ctx, rpc, err := s.ownerRoute(ctx, in.GetOwner(), in.GetRequestId(), false)
	if err != nil {
		return nil, err
	}
	return rpc.Renew(ctx, in)
}

func (s *onlineRPC) Validate(ctx context.Context, in *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	ctx, rpc, err := s.ownerRoute(ctx, in.GetOwner(), "", false)
	if err != nil {
		return nil, err
	}
	return rpc.Validate(ctx, in)
}

func (s *onlineRPC) Release(ctx context.Context, in *pb.ReleaseRequest) (*pb.ReleaseAck, error) {
	if in == nil || !validID(in.RequestId) || len(in.RequestId) > maxBlockRequestBytes || len(in.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid online release")
	}
	if in.Mode == pb.ReleaseMode_RELEASE_MODE_NODE {
		// Online enrollment supplies no physical cleanup receipt authority.
		return nil, status.Error(codes.FailedPrecondition, "independent node cleanup authority not installed")
	}
	if in.Mode != pb.ReleaseMode_RELEASE_MODE_ALLOCATION || in.NodeId != "" || in.CleanupReceiptId != "" {
		return nil, status.Error(codes.InvalidArgument, "explicit allocation closure required")
	}
	cm, r, err := s.domain.read(ctx)
	if err != nil {
		return nil, err
	}
	if r.Selection == nil || !proto.Equal(in.Allocation, s.domain.reservedLease(r).Allocation) {
		return nil, status.Error(codes.Aborted, "allocation identity mismatch")
	}
	input, err := requestDigest(in)
	if err != nil {
		return nil, err
	}
	if r.ReleaseID != "" {
		if r.ReleaseID != in.RequestId || r.ReleaseInput != input {
			return nil, status.Error(codes.AlreadyExists, "closure request already bound")
		}
		return &pb.ReleaseAck{}, nil
	}
	l, err := blockRenewals(cm)
	if err != nil {
		return nil, err
	}
	if in.RequestId == r.ReserveID || in.RequestId == cm.Data["acquireID"] || in.RequestId == cm.Data["releaseID"] || l.Requests[blockRequestDigest(in.RequestId)].Deadline != 0 {
		return nil, status.Error(codes.AlreadyExists, "request key belongs to another operation")
	}
	if r.Busy != nil {
		return nil, status.Error(codes.Unavailable, "native operation outcome retained")
	}
	if r.Closed {
		return nil, status.Error(codes.FailedPrecondition, "domain already closed by native deletion")
	}
	current, err := s.domain.hold(r).read(ctx)
	if err != nil {
		return nil, err
	}
	if current.ResourceVersion != cm.ResourceVersion {
		return nil, status.Error(codes.Aborted, "hold changed during allocation closure")
	}
	switch cm.Data["phase"] {
	case "selected", "published":
		if in.OwnerEpoch != 0 || cm.Data["acquireID"] != "" {
			return nil, status.Error(codes.Aborted, "unacquired allocation epoch mismatch")
		}
	case "released":
		if in.OwnerEpoch != r.Selection.Epoch {
			return nil, status.Error(codes.Aborted, "released allocation epoch mismatch")
		}
	default:
		return nil, status.Error(codes.FailedPrecondition, "live, uncertain or unreleased owner retained")
	}
	r.Closed, r.ReleaseID, r.ReleaseInput = true, in.RequestId, input
	if _, err := s.domain.save(ctx, cm, r); err != nil {
		return nil, err
	}
	return &pb.ReleaseAck{}, nil
}
