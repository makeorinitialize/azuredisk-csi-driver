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
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

// OpenedBlock is independent node evidence, never populated from ValidateRequest.
type OpenedBlock struct {
	Selection    BlockSelection
	Major, Minor uint32
}

// BlockOpenAuthority resolves the immutable, exclusive open for this peer and
// owner epoch. Pin that same open file description and its current use by the
// facade throughout check. Reject closed/reopened or unprovable handles, even
// when the replacement has the same dev_t and Azure GUID. Invoke check once
// synchronously and propagate its error; never retry or return cached evidence.
type BlockOpenAuthority interface {
	WithOpened(context.Context, BlockSelection, [sha256.Size]byte, func(OpenedBlock) error) error
}

// WithOpenedAuthority configures Validate on a copy; nil keeps it disabled.
func (h *BlockHold) WithOpenedAuthority(a BlockOpenAuthority) *BlockHold {
	copy := *h
	copy.opened = a
	return &copy
}

func (s *reservationRPC) Validate(ctx context.Context, in *pb.ValidateRequest) (*pb.ValidateResponse, error) {
	b := s.block
	if b == nil {
		return nil, status.Error(codes.Unimplemented, "opened block validation unavailable")
	}
	o, x := in.GetOwner(), in.GetOpened()
	if o == nil || x == nil || len(in.ProtoReflect().GetUnknown()) != 0 || len(o.ProtoReflect().GetUnknown()) != 0 || len(x.ProtoReflect().GetUnknown()) != 0 ||
		x.StableDeviceId == "" || x.DeviceGeneration == 0 || x.CapacityBytes == 0 || x.ObservedMajor == 0 {
		return nil, status.Error(codes.InvalidArgument, "incomplete opened block observation")
	}
	if err := b.checkOwner(ctx, o.Allocation, o.NodeId, o.OwnerEpoch); err != nil {
		return nil, err
	}
	h := b.h
	if h.opened == nil {
		return nil, status.Error(codes.FailedPrecondition, "independent node open authority not installed")
	}
	cm, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	// Validate is read-only. In particular it must never acquire, renew or
	// change the phase (including when it observes an expired deadline).
	until, err := strconv.ParseInt(cm.Data["deadline"], 10, 64)
	if cm.Data["phase"] != "held" || err != nil || time.Now().Unix() >= until {
		return nil, status.Error(codes.FailedPrecondition, "current live block hold required")
	}
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	checked := false
	var checkErr error
	err = h.opened.WithOpened(ctx, h.s, g.peerDigest, func(e OpenedBlock) error {
		if checked {
			checkErr = status.Error(codes.Unavailable, "ambiguous node open observation")
			return checkErr
		}
		checked = true
		switch {
		case e.Selection != h.s || x.StableDeviceId != e.Selection.DiskUniqueID || x.DeviceGeneration != e.Selection.DeviceGeneration || x.CapacityBytes != uint64(e.Selection.CapacityBytes) || x.ObservedMajor != e.Major || x.ObservedMinor != e.Minor:
			checkErr = status.Error(codes.Aborted, "opened device or owner does not match enrolled origin")
		default:
			checkErr = h.verifyPublished(ctx, b.observe)
		}
		return checkErr
	})
	if checkErr != nil {
		return nil, checkErr
	}
	if err != nil || !checked {
		return nil, status.Error(codes.Unavailable, "node could not confirm the same exclusive open")
	}
	current, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	if current.ResourceVersion != cm.ResourceVersion {
		return nil, status.Error(codes.Aborted, "block hold changed during validation")
	}
	if time.Now().Unix() >= until {
		return nil, status.Error(codes.FailedPrecondition, "block hold expired during validation")
	}
	return &pb.ValidateResponse{}, nil
}
