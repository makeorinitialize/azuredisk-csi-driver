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
	"crypto/tls"
	"crypto/x509"
	"strconv"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

type publishedRPC struct {
	h       *BlockHold
	observe BlockObserver
}

func NewPublishedBlockServer(h *BlockHold, observe BlockObserver, cert tls.Certificate, roots *x509.CertPool, grants []ReservationGrant) (*grpc.Server, error) {
	if h == nil || observe == nil || h.cleanup == nil {
		return nil, status.Error(codes.FailedPrecondition, "block selection, attachment observer and independent cleanup verifier required")
	}
	return (&reservationRPC{block: &publishedRPC{h, observe}}).server(cert, roots, grants)
}

func (b *publishedRPC) allocation() *pb.Allocation {
	s := b.h.s
	return &pb.Allocation{Key: &pb.LeaseKey{Provider: s.Provider, LeaseId: s.AllocationID}, CompositeVolumeId: s.CompositeID, ResourceId: s.ResourceID, StableDeviceId: s.DiskUniqueID, DeviceGeneration: s.DeviceGeneration}
}

func (b *publishedRPC) checkOwner(ctx context.Context, a *pb.Allocation, node string, epoch uint64) error {
	if a.GetKey() == nil || node == "" || epoch == 0 {
		return status.Error(codes.InvalidArgument, "incomplete block owner")
	}
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	if !g.volumes[a.CompositeVolumeId] || g.node != node {
		return status.Error(codes.PermissionDenied, "node or composite outside grant")
	}
	if !proto.Equal(a, b.allocation()) || b.h.s.Owner != (Owner{NodeID: node, Epoch: epoch}) {
		return status.Error(codes.Aborted, "stale block allocation or owner")
	}
	return nil
}

func (b *publishedRPC) acquire(ctx context.Context, in *pb.AcquireRequest) (*pb.Lease, error) {
	o := in.GetOwner()
	if o == nil || !validID(in.RequestId) || len(in.ProtoReflect().GetUnknown()) != 0 || len(o.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "unknown acquire input")
	}
	if err := b.checkOwner(ctx, o.Allocation, o.NodeId, o.OwnerEpoch); err != nil {
		return nil, err
	}
	if in.PreviousOwnerEpoch != 0 {
		return nil, status.Error(codes.Aborted, "first owner only")
	}
	if err := b.h.acquire(ctx, in.RequestId, b.h.s.Binding, b.h.s.Owner, b.observe); err != nil {
		return nil, err
	}
	l, err := b.lookup(ctx, o.Allocation.Key)
	if err != nil {
		return nil, err
	}
	if l.State != pb.LeaseState_LEASE_STATE_ACQUIRED {
		return nil, status.Error(codes.FailedPrecondition, "hold no longer active")
	}
	return l, nil
}

func (b *publishedRPC) lookup(ctx context.Context, k *pb.LeaseKey) (*pb.Lease, error) {
	s := b.h.s
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	if !g.volumes[s.CompositeID] {
		return nil, status.Error(codes.PermissionDenied, "composite outside grant")
	}
	a := b.allocation()
	if !proto.Equal(a.Key, k) {
		return nil, status.Error(codes.NotFound, "unknown block selection")
	}
	cm, err := b.h.read(ctx)
	if err != nil {
		return nil, err
	}
	l := &pb.Lease{Allocation: a, CapacityBytes: uint64(s.CapacityBytes), PoolId: s.PoolID, State: pb.LeaseState_LEASE_STATE_RESERVED}
	switch cm.Data["phase"] {
	case "released":
		l.OwnerEpoch = s.Epoch
	case "published":
	case "expired", "held":
		l.NodeId, l.OwnerEpoch = s.NodeID, s.Epoch
		l.RenewByUnixSeconds, err = strconv.ParseInt(cm.Data["deadline"], 10, 64)
		if err != nil {
			return nil, unavailable(err)
		}
		l.State = pb.LeaseState_LEASE_STATE_HELD
		if cm.Data["phase"] == "held" {
			live, err := b.h.live(ctx, cm)
			if err != nil {
				return nil, err
			}
			if !live {
				return l, nil
			}
			// A stored hold is not by itself evidence that the disk is still
			// attached to the expected VM.
			if err = b.h.acquire(ctx, cm.Data["acquireID"], s.Binding, s.Owner, b.observe); err != nil {
				return nil, err
			}
			l.State = pb.LeaseState_LEASE_STATE_ACQUIRED
		}
	default:
		return nil, status.Error(codes.Unavailable, "block publication not available")
	}
	return l, nil
}

func (s *reservationRPC) Release(ctx context.Context, in *pb.ReleaseRequest) (*pb.ReleaseAck, error) {
	if s.block == nil {
		return nil, status.Error(codes.Unimplemented, "node release unavailable")
	}
	if in == nil || !validID(in.RequestId) || in.Mode != pb.ReleaseMode_RELEASE_MODE_NODE || len(in.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "only explicit NODE cleanup is supported")
	}
	b := s.block
	if err := b.checkOwner(ctx, in.Allocation, in.NodeId, in.OwnerEpoch); err != nil {
		return nil, err
	}
	if err := b.h.release(ctx, in.RequestId, b.h.s.Binding, b.h.s.Owner, in.CleanupReceiptId); err != nil {
		return nil, err
	}
	return &pb.ReleaseAck{}, nil
}
