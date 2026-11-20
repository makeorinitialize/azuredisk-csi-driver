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
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
)

// BlockSelection is trusted immutable enrollment for one ordinary block PV.
// Static selections precede publication; external selections are resolved under
// the shared native admission journal. Never reset/reuse an allocation's history.
type BlockSelection struct {
	Binding
	Owner
	Provider, PoolID                                string
	PV, PVUID, Namespace, PVC, PVCUID, VMID, VMGUID string
}

func (s BlockSelection) valid() bool {
	_, err := armKey(s.ResourceID)
	return err == nil && validID(s.Provider) && validID(s.PoolID) && (attachInput{Binding: s.Binding, Owner: s.Owner}).valid() && validVMID(s.VMID) &&
		validID(s.VMGUID) && validID(s.PV) && validID(s.PVUID) && validID(s.Namespace) && validID(s.PVC) && validID(s.PVCUID)
}

// BlockCleanup must independently resolve an immutable receipt for this exact
// enrollment, node and epoch, proving completed hot-unplug and no further IO.
type BlockCleanup interface {
	VerifyCleanup(context.Context, BlockSelection, string) error
}

type BlockHold struct {
	s       BlockSelection
	maps    coreclient.ConfigMapInterface
	name    string
	uid     types.UID
	cleanup BlockCleanup
	opened  BlockOpenAuthority
	online  *OnlineDomain
	now     func() time.Time
}

func InitialBlockHold(s BlockSelection) (map[string]string, error) {
	if !s.valid() {
		return nil, status.Error(codes.InvalidArgument, "invalid block selection")
	}
	spec, err := json.Marshal(s)
	if err != nil {
		return nil, err
	}
	return map[string]string{"selection.v1": string(spec), "phase": "selected", "receipt": ""}, nil
}

func NewBlockHold(s BlockSelection, maps coreclient.ConfigMapInterface, name string, uid types.UID, cleanup BlockCleanup) (*BlockHold, error) {
	if !s.valid() || maps == nil || name == "" || uid == "" {
		return nil, status.Error(codes.InvalidArgument, "incomplete block hold configuration")
	}
	return &BlockHold{s: s, maps: maps, name: name, uid: uid, cleanup: cleanup}, nil
}
func (h *BlockHold) Selection() BlockSelection { return h.s }
func (h *BlockHold) Matches(id string) bool    { return h != nil && strings.EqualFold(id, h.s.ResourceID) }

func (h *BlockHold) clock() time.Time {
	if h.now != nil {
		return h.now()
	}
	return time.Now()
}

func (h *BlockHold) read(ctx context.Context) (*v1.ConfigMap, error) {
	cm, err := h.maps.Get(ctx, h.name, metav1.GetOptions{})
	if err != nil {
		return nil, unavailable(err)
	}
	spec, _ := json.Marshal(h.s)
	if cm == nil || cm.UID != h.uid || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || cm.Data["selection.v1"] != string(spec) {
		return nil, status.Error(codes.Unavailable, "block hold incarnation unavailable")
	}
	receipt, ok := cm.Data["receipt"]
	if !ok || (cm.Data["phase"] == "released") != (receipt != "") {
		return nil, status.Error(codes.Unavailable, "inconsistent block cleanup history")
	}
	switch cm.Data["phase"] {
	case "held", "renewing", "expired", "released":
		until, err := strconv.ParseInt(cm.Data["deadline"], 10, 64)
		if err != nil || until <= 0 || !validID(cm.Data["acquireID"]) || (cm.Data["phase"] == "released") != (cm.Data["releaseID"] != "") || cm.Data["acquireID"] == cm.Data["releaseID"] {
			return nil, status.Error(codes.Unavailable, "inconsistent block owner history")
		}
	case "selected", "setup", "published", "teardown":
		if cm.Data["acquireID"] != "" || cm.Data["releaseID"] != "" || cm.Data["deadline"] != "" {
			return nil, status.Error(codes.Unavailable, "unexpected block owner history")
		}
	default:
		return nil, status.Error(codes.Unavailable, "unknown block hold phase")
	}
	if _, err := blockRenewals(cm); err != nil {
		return nil, err
	}
	if h.online != nil {
		if err := h.online.checkHold(ctx, cm); err != nil {
			return nil, err
		}
	}
	return cm, nil
}
func (h *BlockHold) move(ctx context.Context, cm *v1.ConfigMap, phase, receipt string) (*v1.ConfigMap, error) {
	next := cm.DeepCopy()
	next.Data["phase"], next.Data["receipt"] = phase, receipt
	saved, err := h.maps.Update(ctx, next, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, status.Error(codes.Aborted, "concurrent block hold operation")
	}
	if err != nil {
		return nil, unavailable(err)
	}
	if saved == nil || saved.UID != h.uid || saved.DeletionTimestamp != nil || saved.ResourceVersion == "" || saved.ResourceVersion == cm.ResourceVersion ||
		!maps.Equal(saved.Data, next.Data) {
		return nil, status.Error(codes.Unavailable, "block hold transition unconfirmed")
	}
	return saved, nil
}

// BeginSetup brackets ordinary ControllerPublish / node setup. An interrupted
// setup leaves a durable busy marker, not a purported successful publication.
func (h *BlockHold) BeginSetup(ctx context.Context, controllerPublish bool) (func() error, error) {
	cm, err := h.read(ctx)
	if err != nil {
		return nil, err
	}
	if cm.Data["phase"] != "published" && !(controllerPublish && cm.Data["phase"] == "selected") {
		return nil, status.Error(codes.FailedPrecondition, "block setup not admitted")
	}
	busy, err := h.move(ctx, cm, "setup", "")
	if err != nil {
		return nil, err
	}
	return func() error { _, err := h.move(ctx, busy, "published", ""); return err }, nil
}

// EnterTeardown permanently closes hold admission BEFORE any physical effect.
// No process-local unlock, timeout, lost response or restart can reopen it.
func (h *BlockHold) EnterTeardown(ctx context.Context) error {
	cm, err := h.read(ctx)
	if err != nil {
		return err
	}
	switch cm.Data["phase"] {
	case "released", "teardown":
		return nil
	case "published", "selected":
		_, err = h.move(ctx, cm, "teardown", "")
		return err
	default:
		return status.Error(codes.FailedPrecondition, "block hold or setup retained; teardown denied")
	}
}

// Resolve is an internal trusted-call boundary, not a raw device/open grant.
// The driver's observer must verify the live PV/PVC binding and read both RPs.
func (h *BlockHold) Resolve(ctx context.Context, b Binding, o Owner, observe func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error)) error {
	return h.acquire(ctx, "internal-acquire", b, o, observe)
}

type BlockObserver func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error)

func (h *BlockHold) acquire(ctx context.Context, key string, b Binding, o Owner, observe BlockObserver) error {
	if h.s.Binding != b || h.s.Owner != o {
		return status.Error(codes.Aborted, "block owner or allocation mismatch")
	}
	cm, err := h.read(ctx)
	if err != nil {
		return err
	}
	renewals, err := blockRenewals(cm)
	if err != nil {
		return err
	}
	if key == cm.Data["releaseID"] || renewals.Requests[blockRequestDigest(key)].Deadline != 0 {
		return status.Error(codes.AlreadyExists, "key belongs to another block operation")
	}
	if cm.Data["acquireID"] != "" && cm.Data["acquireID"] != key {
		return status.Error(codes.FailedPrecondition, "another request owns block hold")
	}
	if cm.Data["phase"] != "published" && cm.Data["phase"] != "held" {
		return status.Error(codes.FailedPrecondition, "block publication not available")
	}
	if err = h.verifyPublished(ctx, observe); err != nil {
		return err
	}
	if cm.Data["phase"] == "held" {
		current, err := h.read(ctx)
		if err != nil {
			return err
		}
		if cm.ResourceVersion != current.ResourceVersion {
			return status.Error(codes.Aborted, "block hold changed during replay")
		}
		live, err := h.live(ctx, current)
		if err != nil {
			return err
		}
		if !live {
			return status.Error(codes.FailedPrecondition, "block hold expired; cleanup still required")
		}
		return nil
	}
	cm = cm.DeepCopy()
	cm.Data["acquireID"] = key
	cm.Data["deadline"] = strconv.FormatInt(h.clock().Add(time.Minute).Unix(), 10)
	_, err = h.move(ctx, cm, "held", "")
	return err
}

func (h *BlockHold) live(ctx context.Context, cm *v1.ConfigMap) (bool, error) {
	until, err := strconv.ParseInt(cm.Data["deadline"], 10, 64)
	renewals, ledgerErr := blockRenewals(cm)
	if ledgerErr != nil {
		return false, ledgerErr
	}
	if renewals.Pending != nil {
		until = renewals.Pending.PreviousDeadline
	}
	if err == nil && h.clock().Unix() < until {
		return true, nil
	}
	_, err = h.move(ctx, cm, "expired", "")
	return false, err
}

func (h *BlockHold) verifyPublished(ctx context.Context, observe BlockObserver) error {
	if observe == nil {
		return status.Error(codes.Unavailable, "no block observer")
	}
	d, vm, err := observe(ctx, h.s)
	if err != nil {
		return err
	}
	if !strings.EqualFold(vm.ID, h.s.VMID) || vm.InstanceGUID != h.s.VMGUID {
		return status.Error(codes.Aborted, "block VM incarnation changed")
	}
	present, err := vm.hasDisk(h.s.ResourceID)
	if err != nil {
		return err
	}
	if !present || d == nil || d.Properties == nil || !strings.EqualFold(ptr.Deref(d.Properties.ProvisioningState, ""), "Succeeded") || d.Properties.PropertyUpdatesInProgress != nil {
		return status.Error(codes.Unavailable, "ordinary block attachment not confirmed")
	}
	return verifyAttachment(h.s.Binding, Attachment{Disk: d, VMResourceID: vm.ID, Mode: azureconstants.AttachModeControllerDriven})
}

func (h *BlockHold) ReleaseNode(ctx context.Context, b Binding, o Owner, receipt string) error {
	return h.release(ctx, "internal-release", b, o, receipt)
}
func (h *BlockHold) release(ctx context.Context, key string, b Binding, o Owner, receipt string) error {
	if h.s.Binding != b || h.s.Owner != o {
		return status.Error(codes.Aborted, "block owner or allocation mismatch")
	}
	if !validID(receipt) {
		return status.Error(codes.InvalidArgument, "missing cleanup receipt")
	}
	cm, err := h.read(ctx)
	if err != nil {
		return err
	}
	renewals, err := blockRenewals(cm)
	if err != nil {
		return err
	}
	if cm.Data["acquireID"] == key || renewals.Requests[blockRequestDigest(key)].Deadline != 0 || (cm.Data["releaseID"] == key && cm.Data["receipt"] != receipt) {
		return status.Error(codes.AlreadyExists, "key bound to different request")
	}
	if cm.Data["phase"] == "released" && cm.Data["receipt"] == receipt && cm.Data["releaseID"] == key {
		return nil
	}
	if (cm.Data["phase"] != "held" && cm.Data["phase"] != "renewing" && cm.Data["phase"] != "expired") || h.cleanup == nil {
		return status.Error(codes.FailedPrecondition, "no releasable hold or cleanup verifier")
	}
	if err = h.cleanup.VerifyCleanup(ctx, h.s, receipt); err != nil {
		return unavailable(err)
	}
	cm = cm.DeepCopy()
	cm.Data["releaseID"] = key
	_, err = h.move(ctx, cm, "released", receipt)
	return err
}
