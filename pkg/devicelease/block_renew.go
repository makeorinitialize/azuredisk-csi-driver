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
	"encoding/json"
	"strconv"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

const (
	blockRenewalKey      = "renewals.v1"
	maxBlockRenewals     = 2048
	maxBlockRequestBytes = 1024
)

type blockRenewal struct {
	Input    string `json:"input"`
	Deadline int64  `json:"deadline"`
}

type blockRenewalLedger struct {
	Version  int                     `json:"version"`
	Requests map[string]blockRenewal `json:"requests"`
	Pending  *blockRenewalPending    `json:"pending,omitempty"`
}

type blockRenewalPending struct {
	Request          string `json:"request"`
	PreviousDeadline int64  `json:"previousDeadline"`
}

func blockRequestDigest(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// Missing renewal history is the original one-shot journal. Migrate only in
// the same CAS that admits the first renewal candidate, preserving all data.
// Canonical encoding rejects unknown fields/versions rather than discarding
// history during a later CAS. Never prune successful request IDs.
func blockRenewals(cm *v1.ConfigMap) (blockRenewalLedger, error) {
	l := blockRenewalLedger{Version: 1, Requests: map[string]blockRenewal{}}
	raw, exists := cm.Data[blockRenewalKey]
	if !exists && cm.Data["phase"] != "renewing" {
		return l, nil
	}
	bad := status.Error(codes.Unavailable, "incompatible block renewal history")
	if len(raw) > 512*1024 || json.Unmarshal([]byte(raw), &l) != nil || l.Version != 1 || l.Requests == nil || len(l.Requests) > maxBlockRenewals {
		return l, bad
	}
	canonical, err := json.Marshal(l)
	if err != nil || string(canonical) != raw {
		return l, bad
	}
	until, err := strconv.ParseInt(cm.Data["deadline"], 10, 64)
	if err != nil || (cm.Data["phase"] != "held" && cm.Data["phase"] != "renewing" && cm.Data["phase"] != "expired" && cm.Data["phase"] != "released") {
		return l, bad
	}
	if l.Pending != nil {
		if cm.Data["phase"] == "held" || l.Pending.PreviousDeadline <= 0 || l.Pending.PreviousDeadline > until || l.Requests[l.Pending.Request].Deadline != until {
			return l, bad
		}
	} else if cm.Data["phase"] == "renewing" {
		return l, bad
	}
	for key, r := range l.Requests {
		k, e1 := hex.DecodeString(key)
		input, e2 := hex.DecodeString(r.Input)
		if e1 != nil || e2 != nil || len(k) != sha256.Size || len(input) != sha256.Size || r.Deadline <= 0 || r.Deadline > until ||
			key == blockRequestDigest(cm.Data["acquireID"]) || key == blockRequestDigest(cm.Data["releaseID"]) {
			return l, bad
		}
		if l.Pending != nil && key != l.Pending.Request && r.Deadline > l.Pending.PreviousDeadline {
			return l, bad
		}
	}
	return l, nil
}

func (s *reservationRPC) Renew(ctx context.Context, in *pb.RenewRequest) (*pb.Lease, error) {
	b := s.block
	if b == nil {
		return nil, status.Error(codes.Unimplemented, "published block renewal unavailable")
	}
	o := in.GetOwner()
	if o == nil || !validID(in.RequestId) || len(in.RequestId) > maxBlockRequestBytes ||
		len(in.ProtoReflect().GetUnknown()) != 0 || len(o.ProtoReflect().GetUnknown()) != 0 ||
		o.GetAllocation().GetKey() == nil || len(o.Allocation.ProtoReflect().GetUnknown()) != 0 || len(o.Allocation.Key.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "invalid block renewal")
	}
	g := ctx.Value(grantContextKey{}).(reservationGrant)
	if !g.volumes[o.Allocation.CompositeVolumeId] || g.node != o.NodeId {
		return nil, status.Error(codes.PermissionDenied, "node or composite outside grant")
	}
	h := b.h
	cm, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	ledger, err := blockRenewals(cm)
	if err != nil {
		return nil, err
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(in)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid block renewal input")
	}
	digest, input := blockRequestDigest(in.RequestId), blockRequestDigest(string(wire))
	record, replay := ledger.Requests[digest]
	if in.RequestId == cm.Data["acquireID"] || in.RequestId == cm.Data["releaseID"] || (replay && record.Input != input) {
		return nil, status.Error(codes.AlreadyExists, "key bound to different block request")
	}
	if err = b.checkOwner(ctx, o.Allocation, o.NodeId, o.OwnerEpoch); err != nil {
		return nil, err
	}
	if cm.Data["phase"] != "held" && cm.Data["phase"] != "renewing" {
		return nil, status.Error(codes.FailedPrecondition, "current live block hold required")
	}
	live, err := h.live(ctx, cm)
	if err != nil {
		return nil, err
	}
	if !live {
		return nil, status.Error(codes.FailedPrecondition, "block hold expired; cleanup still required")
	}
	if ledger.Pending != nil && ledger.Pending.Request != digest {
		return nil, status.Error(codes.Unavailable, "pending block renewal; retry its original request")
	}
	if !replay && len(ledger.Requests) >= maxBlockRenewals {
		return nil, status.Error(codes.ResourceExhausted, "block renewal history full; cleanup required")
	}
	// Renew proves continuing origin attachment, not device-open custody.
	// The observer independently rechecks PV/PVC and both Azure RPs, exactly
	// as Acquire does; caller claims or a stored deadline are not evidence.
	if err = h.verifyPublished(ctx, b.observe); err != nil {
		return nil, err
	}
	current, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	if current.ResourceVersion != cm.ResourceVersion {
		return nil, status.Error(codes.Aborted, "block hold changed during renewal")
	}
	live, err = h.live(ctx, current)
	if err != nil {
		return nil, err
	}
	if !live {
		return nil, status.Error(codes.FailedPrecondition, "block hold expired during renewal")
	}
	if !replay {
		until, _ := strconv.ParseInt(current.Data["deadline"], 10, 64)
		deadline := max(until, time.Now().Add(time.Minute).Unix())
		ledger.Requests[digest] = blockRenewal{Input: input, Deadline: deadline}
		ledger.Pending = &blockRenewalPending{Request: digest, PreviousDeadline: until}
		data, err := json.Marshal(ledger)
		if err != nil {
			return nil, unavailable(err)
		}
		next := current.DeepCopy()
		next.Data[blockRenewalKey] = string(data)
		next.Data["deadline"] = strconv.FormatInt(deadline, 10)
		current, err = h.move(ctx, next, "renewing", "")
		if err != nil {
			return nil, err
		}
	}
	if ledger.Pending != nil {
		// A persisted candidate is not yet a renewed hold. Observe its CAS
		// while the previous deadline is still live before authorizing the
		// confirmation CAS. A lost/late candidate reply or a restart cannot
		// substitute its new deadline for that proof of timely observation.
		live, err = h.live(ctx, current)
		if err != nil {
			return nil, err
		}
		if !live {
			return nil, status.Error(codes.FailedPrecondition, "prior block hold expired during renewal commit")
		}
		ledger.Pending = nil
		data, err := json.Marshal(ledger)
		if err != nil {
			return nil, unavailable(err)
		}
		next := current.DeepCopy()
		next.Data[blockRenewalKey] = string(data)
		current, err = h.move(ctx, next, "held", "")
		if err != nil {
			return nil, err
		}
	}
	until, _ := strconv.ParseInt(current.Data["deadline"], 10, 64)
	if time.Now().Unix() >= until {
		_, err = h.live(ctx, current)
		if err != nil {
			return nil, err
		}
		return nil, status.Error(codes.FailedPrecondition, "block hold expired before renewal reply")
	}
	return &pb.Lease{Allocation: b.allocation(), PoolId: h.s.PoolID, CapacityBytes: uint64(h.s.CapacityBytes),
		State: pb.LeaseState_LEASE_STATE_ACQUIRED, NodeId: h.s.NodeID, OwnerEpoch: h.s.Epoch, RenewByUnixSeconds: until}, nil
}
