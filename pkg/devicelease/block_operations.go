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
	"encoding/hex"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

const maxBlockRequestBytes = 1024

func blockRequestDigest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (s *reservationRPC) Renew(ctx context.Context, in *pb.RenewRequest) (*pb.Lease, error) {
	if s.block == nil {
		return nil, status.Error(codes.Unimplemented, "published block renewal unavailable")
	}
	o := in.GetOwner()
	if o == nil || !validID(in.RequestId) || len(in.RequestId) > maxBlockRequestBytes ||
		len(in.ProtoReflect().GetUnknown()) != 0 || len(o.ProtoReflect().GetUnknown()) != 0 ||
		o.GetAllocation().GetKey() == nil || len(o.Allocation.ProtoReflect().GetUnknown()) != 0 || len(o.Allocation.Key.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid block renewal")
	}
	if err := s.block.checkOwner(ctx, o.Allocation, o.NodeId, o.OwnerEpoch); err != nil {
		return nil, err
	}
	return nil, status.Error(codes.FailedPrecondition, "explicit-release holds do not support or require renewal")
}
