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
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"maps"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

const onlineDomainKey = "external-domain.v1"

// Class/size values are optional administrator restrictions, not ownership or
// provisioning requests. The external source journal pins the actual values.
type BackingConfig struct {
	Namespace          string    `json:"namespace"`
	NamespaceUID       types.UID `json:"namespaceUID"`
	NodeName           string    `json:"nodeName"`
	NodeUID            types.UID `json:"nodeUID"`
	OriginStorageClass string    `json:"originStorageClass,omitempty"`
	CacheStorageClass  string    `json:"cacheStorageClass,omitempty"`
	OriginBytes        int64     `json:"originBytes,omitempty"`
	CacheBytes         int64     `json:"cacheBytes,omitempty"`
	OriginDriver       string    `json:"originDriver,omitempty"`
	CacheDriver        string    `json:"cacheDriver,omitempty"`
}

func (c BackingConfig) EffectiveOriginDriver() string {
	if c.OriginDriver == "" {
		return "disk.csi.azure.com"
	}
	return c.OriginDriver
}
func (c BackingConfig) EffectiveCacheDriver() string {
	if c.CacheDriver == "" {
		return "localdisk.csi.acstor.io"
	}
	return c.CacheDriver
}
func (c BackingConfig) validDrivers() bool {
	o, k := c.EffectiveOriginDriver(), c.EffectiveCacheDriver()
	return len(o) <= 63 && len(k) <= 63 && len(validation.IsDNS1123Subdomain(o)) == 0 && len(validation.IsDNS1123Subdomain(k)) == 0 &&
		o != k && o != "localdisk.csi.acstor.io" && k != "disk.csi.azure.com"
}
func (c BackingConfig) same(other BackingConfig) bool {
	c.OriginDriver, c.CacheDriver = c.EffectiveOriginDriver(), c.EffectiveCacheDriver()
	other.OriginDriver, other.CacheDriver = other.EffectiveOriginDriver(), other.EffectiveCacheDriver()
	return c == other
}

type OnlineIdentity struct {
	ProviderID      string        `json:"providerID"`
	PoolID          string        `json:"poolID"`
	CompositeCRDUID types.UID     `json:"compositeCRDUID"`
	Backing         BackingConfig `json:"backing"`
}

func (i OnlineIdentity) same(o OnlineIdentity) bool {
	return i.ProviderID == o.ProviderID && i.PoolID == o.PoolID && i.CompositeCRDUID == o.CompositeCRDUID && i.Backing.same(o.Backing)
}
func (i OnlineIdentity) valid() bool {
	b := i.Backing
	return validID(i.ProviderID) && validID(i.PoolID) && i.CompositeCRDUID != "" && b.validDrivers() &&
		b.Namespace != "" && b.Namespace != "default" && b.NamespaceUID != "" && b.NodeName != "" && b.NodeUID != "" &&
		b.OriginBytes >= 0 && b.CacheBytes >= 0 && b.OriginBytes%512 == 0 && b.CacheBytes%512 == 0
}

type nativeIntent struct{ ID, Method, Digest, ResourceID string }
type nativeCompletion struct {
	Intent  nativeIntent
	Outcome string
}
type onlineRecord struct {
	Version                 int
	Identity                OnlineIdentity
	Busy                    *nativeIntent
	CompositeID, Owner      string
	Backing                 *backingJournal
	Selection               *BlockSelection
	ReserveID, ReserveInput string
	Closed                  bool
	ReleaseID, ReleaseInput string
	LastNative              *nativeCompletion `json:",omitempty"`
}

// Keep the API's actual conflict distinct from generic ABORTED/transport errors.
// Only this confirmed lost-CAS result permits a completion write retry.
type nativeJournalConflict struct{ cause error }

func (e *nativeJournalConflict) Error() string { return "concurrent native admission or hold mutation" }
func (e *nativeJournalConflict) Unwrap() error { return e.cause }
func (e *nativeJournalConflict) GRPCStatus() *status.Status {
	return status.New(codes.Aborted, e.Error())
}

type OnlineDomain struct {
	identity OnlineIdentity
	maps     coreclient.ConfigMapInterface
	name     string
	uid      types.UID
	kube     kubernetes.Interface
	dynamic  dynamic.Interface
}

func InitialOnlineDomain(i OnlineIdentity) (map[string]string, error) {
	if !i.valid() {
		return nil, status.Error(codes.InvalidArgument, "incomplete external source domain")
	}
	b, err := json.Marshal(onlineRecord{Version: 2, Identity: i})
	return map[string]string{onlineDomainKey: string(b)}, err
}
func NewOnlineDomain(i OnlineIdentity, maps coreclient.ConfigMapInterface, name string, uid types.UID, kube kubernetes.Interface, dyn dynamic.Interface) (*OnlineDomain, error) {
	if !i.valid() || maps == nil || name == "" || uid == "" || kube == nil || dyn == nil {
		return nil, status.Error(codes.InvalidArgument, "pinned domain journal and direct Kubernetes clients required")
	}
	return &OnlineDomain{identity: i, maps: maps, name: name, uid: uid, kube: kube, dynamic: dyn}, nil
}

func (d *OnlineDomain) read(ctx context.Context) (*core.ConfigMap, onlineRecord, error) {
	cm, err := d.maps.Get(ctx, d.name, metav1.GetOptions{})
	var r onlineRecord
	if err != nil {
		return nil, r, unavailable(err)
	}
	bad := status.Error(codes.Unavailable, "external hold journal unavailable or incompatible")
	if cm == nil || cm.UID != d.uid || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil || len(cm.Data[onlineDomainKey]) > 768*1024 ||
		json.Unmarshal([]byte(cm.Data[onlineDomainKey]), &r) != nil || r.Version != 2 || !r.Identity.same(d.identity) {
		return nil, r, bad
	}
	data, _ := json.Marshal(r)
	if string(data) != cm.Data[onlineDomainKey] || (r.Selection == nil) != (r.Backing == nil) || (r.Selection == nil) != (cm.Data["selection.v1"] == "") {
		return nil, r, bad
	}
	if r.Selection != nil {
		if err := checkBlockHoldData(cm); err != nil {
			return nil, r, err
		}
		s := r.Selection
		spec, _ := json.Marshal(s)
		if !s.valid() || cm.Data["selection.v1"] != string(spec) || r.CompositeID != s.CompositeID || r.Owner != r.Backing.Owner ||
			r.ReserveID == "" || r.ReserveInput == "" || r.Backing.Origin.Handle != s.ResourceID || r.Backing.Origin.Bytes != s.CapacityBytes ||
			r.Backing.Origin.PVName != s.PV || string(r.Backing.Origin.PVUID) != s.PVUID || r.Backing.Origin.Name != s.PVC ||
			string(r.Backing.Origin.UID) != s.PVCUID || !r.Backing.Config.matches(d.identity.Backing) ||
			r.Backing.Version != 1 || r.Backing.Origin.Driver != d.identity.Backing.EffectiveOriginDriver() ||
			r.Backing.Cache.Driver != d.identity.Backing.EffectiveCacheDriver() ||
			s.AllocationID != "external-"+blockRequestDigest(string(d.uid)+"\x00"+r.Owner) {
			return nil, r, bad
		}
	} else {
		for _, key := range []string{"hold-policy.v1", "deadline", "phase", "receipt", "acquireID", "releaseID", "renewals.v1"} {
			if _, exists := cm.Data[key]; exists {
				return nil, r, bad
			}
		}
	}
	if r.ReleaseID != "" && (!r.Closed || r.ReleaseInput == "") {
		return nil, r, bad
	}
	if r.LastNative != nil && (r.LastNative.Intent.ID == "" || r.LastNative.Intent.Method == "" || r.LastNative.Intent.Digest == "" || len(r.LastNative.Outcome) != 64) {
		return nil, r, bad
	}
	return cm, r, nil
}
func (d *OnlineDomain) save(ctx context.Context, cm *core.ConfigMap, r onlineRecord) (*core.ConfigMap, error) {
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	next := cm.DeepCopy()
	next.Data[onlineDomainKey] = string(data)
	saved, err := d.maps.Update(ctx, next, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return nil, &nativeJournalConflict{cause: err}
	}
	if err != nil {
		return nil, unavailable(err)
	}
	if saved == nil || saved.UID != d.uid || saved.DeletionTimestamp != nil || saved.ResourceVersion == "" || saved.ResourceVersion == cm.ResourceVersion || !maps.Equal(saved.Data, next.Data) {
		return nil, status.Error(codes.Unavailable, "external hold commit unconfirmed")
	}
	return saved, nil
}
func (d *OnlineDomain) Check(ctx context.Context) error {
	_, r, err := d.read(ctx)
	if err != nil {
		return err
	}
	// Retained ownership must remain observable/cleanable if users delete
	// source metadata, cordon a node, or delete the CompositeVolume.
	if r.Selection != nil {
		return nil
	}
	if err := d.checkDomainObjects(ctx); err != nil {
		return err
	}
	_, _, err = d.checkScope(ctx)
	return err
}

type nativeContextKey struct{}
type nativeToken struct {
	domain *OnlineDomain
	id     string
	method string
	digest string
}

func (d *OnlineDomain) NativeAdmitted(ctx context.Context) bool {
	t, ok := ctx.Value(nativeContextKey{}).(nativeToken)
	return ok && t.domain == d
}

// Read-only statistics do not need a mutation token and must not strand a busy
// marker on a missing ordinary mount. A valid durable domain is still required.
func (d *OnlineDomain) AllowNativeRead(ctx context.Context, id string) error {
	_, r, err := d.read(ctx)
	if err != nil {
		return err
	}
	if nativeTouches(r, "NodeGetVolumeStats", id) {
		return status.Error(codes.FailedPrecondition, "source reserved by composite")
	}
	return nil
}

func nativeTouches(r onlineRecord, method, id string) bool {
	if r.Selection == nil || r.Closed {
		return false
	}
	if id == "" {
		return true
	}
	if strings.EqualFold(id, r.Selection.ResourceID) {
		return true
	}
	if method == "ControllerPublishVolume" || method == "ControllerUnpublishVolume" {
		a, b := id, r.Selection.ResourceID
		return strings.EqualFold(a[strings.LastIndexByte(a, '/')+1:], b[strings.LastIndexByte(b, '/')+1:])
	}
	return false
}

// All native mutations cross this durable barrier, including operations on
// not-yet-enrolled disks. Missing allocation records are never an exclusion
// proof. This prototype serializes native work; ambiguous effects stay busy.
func (d *OnlineDomain) BeginNative(ctx context.Context, method, digest, id, _, _ string) (context.Context, *BlockHold, error) {
	if !validID(digest) {
		return ctx, nil, status.Error(codes.InvalidArgument, "native request binding required")
	}
	cm, r, err := d.read(ctx)
	if err != nil {
		return ctx, nil, err
	}
	if r.Busy != nil {
		return ctx, nil, status.Error(codes.Unavailable, "native operation outcome retained")
	}
	switch method {
	case "CreateVolume", "DeleteVolume", "ControllerPublishVolume", "ControllerUnpublishVolume", "ControllerModifyVolume", "ControllerExpandVolume",
		"NodeStageVolume", "NodePublishVolume", "NodeUnstageVolume", "NodeUnpublishVolume", "NodeExpandVolume", "CreateSnapshot":
	default:
		return ctx, nil, status.Error(codes.Unimplemented, "unsupported native operation")
	}
	if nativeTouches(r, method, id) && method != "CreateVolume" {
		if !strings.EqualFold(id, r.Selection.ResourceID) {
			return ctx, nil, status.Error(codes.FailedPrecondition, "ambiguous selected disk basename")
		}
		switch method {
		case "ControllerPublishVolume", "NodeStageVolume", "NodePublishVolume":
			if cm.Data["acquireID"] != "" || cm.Data["phase"] != "published" && !(method == "ControllerPublishVolume" && cm.Data["phase"] == "selected") {
				return ctx, nil, status.Error(codes.FailedPrecondition, "hold retained; higher-epoch reactivation unsupported")
			}
			p, err := d.resolveBacking(ctx, r.CompositeID)
			if err != nil {
				return ctx, nil, err
			}
			if !p.Journal.same(*r.Backing) || !strings.EqualFold(p.VMID, r.Selection.VMID) {
				return ctx, nil, backingIdentityError()
			}
		case "ControllerUnpublishVolume", "NodeUnstageVolume", "NodeUnpublishVolume":
			if cm.Data["phase"] != "released" && cm.Data["acquireID"] != "" {
				return ctx, nil, status.Error(codes.FailedPrecondition, "verified node-use release required before native teardown")
			}
			if cm.Data["phase"] != "released" && cm.Data["phase"] != "selected" && cm.Data["phase"] != "published" {
				return ctx, nil, status.Error(codes.FailedPrecondition, "uncertain setup remains retained")
			}
		default:
			return ctx, nil, status.Error(codes.FailedPrecondition, "external source remains reserved by composite")
		}
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return ctx, nil, unavailable(err)
	}
	r.Busy = &nativeIntent{hex.EncodeToString(nonce[:]), method, digest, id}
	if _, err := d.save(ctx, cm, r); err != nil {
		return ctx, nil, err
	}
	ctx = context.WithValue(ctx, nativeContextKey{}, nativeToken{d, r.Busy.ID, method, digest})
	if r.Closed {
		return ctx, nil, nil
	}
	return ctx, d.hold(r), nil
}

// Ordinary CreateVolume still belongs to the underlying driver. Once it
// resolves all target/source IDs, reject collision with a retained hold before
// any ARM effect. This stores no creation/provenance/format authority.
func (d *OnlineDomain) CheckNativeCreateTargets(ctx context.Context, ids ...string) error {
	t, ok := ctx.Value(nativeContextKey{}).(nativeToken)
	if !ok || t.domain != d {
		return status.Error(codes.Unavailable, "native admission required")
	}
	cm, r, err := d.read(ctx)
	if err != nil {
		return err
	}
	if r.Busy == nil || r.Busy.ID != t.id || r.Busy.Method != "CreateVolume" || len(ids) == 0 {
		return status.Error(codes.Aborted, "native create admission changed")
	}
	for _, id := range ids {
		if nativeTouches(r, "CreateVolume", id) {
			r.Busy = nil
			if _, err := d.save(ctx, cm, r); err != nil {
				return err
			}
			return status.Error(codes.FailedPrecondition, "native create/clone collides with retained external source")
		}
	}
	r.Busy.ResourceID = ids[0]
	_, err = d.save(ctx, cm, r)
	return err
}

func (d *OnlineDomain) EndNative(ctx context.Context, success bool, resultID string, response []byte) error {
	t, ok := ctx.Value(nativeContextKey{}).(nativeToken)
	if !ok || t.domain != d {
		return status.Error(codes.Unavailable, "native admission required")
	}
	if !success {
		return nil
	}
	outcomeBytes, err := json.Marshal(struct {
		ResultID string
		Response []byte
	}{resultID, response})
	if err != nil {
		return unavailable(err)
	}
	outcome := blockRequestDigest(string(outcomeBytes))
	var expected *nativeIntent
	for attempts := 0; attempts < 16; attempts++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		cm, r, err := d.read(ctx)
		if err != nil {
			return err
		}
		if r.LastNative != nil && r.LastNative.Intent.ID == t.id {
			return matchNativeCompletion(t, expected, outcome, r.LastNative)
		}
		if r.Busy == nil || r.Busy.ID != t.id || r.Busy.Method != t.method || r.Busy.Digest != t.digest ||
			expected != nil && *r.Busy != *expected {
			return status.Error(codes.Aborted, "native admission changed before completion")
		}
		if expected == nil {
			copy := *r.Busy
			expected = &copy
		}
		if r.Selection != nil && !r.Closed && strings.EqualFold(expected.ResourceID, r.Selection.ResourceID) &&
			expected.Method == "ControllerUnpublishVolume" && cm.Data["acquireID"] == "" {
			cm = cm.DeepCopy()
			cm.Data["phase"] = "selected"
		}
		r.LastNative = &nativeCompletion{Intent: *expected, Outcome: outcome}
		r.Busy = nil
		if _, err = d.save(ctx, cm, r); err == nil {
			return nil
		}
		var conflict *nativeJournalConflict
		if errors.As(err, &conflict) {
			// Reread the entire current object, preserving concurrent hold
			// and release history. Never invoke the native handler again.
			continue
		}
		// A lost/unknown reply may conceal a committed completion. Reconcile
		// only its exact receipt; do not blindly repeat even the CAS.
		_, current, readErr := d.read(ctx)
		if readErr != nil {
			return readErr
		}
		if current.LastNative != nil && current.LastNative.Intent.ID == t.id {
			return matchNativeCompletion(t, expected, outcome, current.LastNative)
		}
		return status.Error(codes.Unavailable, "native completion outcome unconfirmed; effect must not be repeated")
	}
	return status.Error(codes.Aborted, "native completion CAS contention; admission retained")
}

func matchNativeCompletion(t nativeToken, expected *nativeIntent, outcome string, done *nativeCompletion) error {
	if done.Intent.ID != t.id || done.Intent.Method != t.method || done.Intent.Digest != t.digest || done.Outcome != outcome ||
		expected != nil && done.Intent != *expected {
		return status.Error(codes.Aborted, "native completion history bound to a different operation/outcome")
	}
	return nil
}

func (d *OnlineDomain) hold(r onlineRecord) *BlockHold {
	if r.Selection == nil {
		return nil
	}
	return &BlockHold{s: *r.Selection, maps: d.maps, name: d.name, uid: d.uid, online: d}
}
func (d *OnlineDomain) checkHold(ctx context.Context, cm *core.ConfigMap) error {
	var r onlineRecord
	if json.Unmarshal([]byte(cm.Data[onlineDomainKey]), &r) != nil || r.Version != 2 || !r.Identity.same(d.identity) {
		return status.Error(codes.Unavailable, "external hold domain unavailable")
	}
	data, _ := json.Marshal(r)
	if string(data) != cm.Data[onlineDomainKey] {
		return status.Error(codes.Unavailable, "incompatible external hold history")
	}
	if t, native := ctx.Value(nativeContextKey{}).(nativeToken); native && t.domain == d {
		if r.Busy == nil || r.Busy.ID != t.id {
			return status.Error(codes.Aborted, "native admission changed")
		}
		return nil
	}
	if r.Busy != nil && nativeTouches(r, r.Busy.Method, r.Busy.ResourceID) {
		return status.Error(codes.Unavailable, "native operation affecting source retained")
	}
	if r.Closed {
		return status.Error(codes.FailedPrecondition, "allocation authorization permanently closed")
	}
	return nil
}
func (d *OnlineDomain) ForceRetryAllowed(ctx context.Context, id string) (bool, error) {
	t, ok := ctx.Value(nativeContextKey{}).(nativeToken)
	if !ok || t.domain != d {
		return false, status.Error(codes.Unavailable, "native detach admission required")
	}
	_, r, err := d.read(ctx)
	if err != nil {
		return false, err
	}
	if r.Busy == nil || r.Busy.ID != t.id {
		return false, status.Error(codes.Aborted, "native detach admission changed")
	}
	return r.Selection == nil || !strings.EqualFold(id, r.Selection.ResourceID), nil
}

func requestDigest(in proto.Message) (string, error) {
	b, err := (proto.MarshalOptions{Deterministic: true}).Marshal(in)
	return blockRequestDigest(string(b)), err
}

func (d *OnlineDomain) reserve(ctx context.Context, in *pb.ReserveRequest, observe BlockObserver) (*pb.Lease, error) {
	if in == nil || !validID(in.RequestId) || len(in.RequestId) > maxBlockRequestBytes || in.Pool == nil || in.CapacityBytes == 0 ||
		len(in.ProtoReflect().GetUnknown()) != 0 || len(in.Pool.ProtoReflect().GetUnknown()) != 0 ||
		in.Pool.Provider != d.identity.ProviderID || in.Pool.PoolId != d.identity.PoolID {
		return nil, status.Error(codes.InvalidArgument, "exact external source pool and positive size required")
	}
	cm, r, err := d.read(ctx)
	if err != nil {
		return nil, err
	}
	digest, err := requestDigest(in)
	if err != nil {
		return nil, err
	}
	if r.ReserveID != "" && (r.ReserveID != in.RequestId || r.ReserveInput != digest) {
		return nil, status.Error(codes.AlreadyExists, "reservation input already bound")
	}
	if r.Closed {
		return nil, status.Error(codes.FailedPrecondition, "allocation authorization closed")
	}
	if r.Selection != nil {
		return d.reservedLease(r), nil
	}
	if r.Busy != nil {
		return nil, status.Error(codes.Unavailable, "native operation must finish before first hold")
	}
	proof, err := d.resolveBacking(ctx, in.CompositeVolumeId)
	if err != nil {
		return nil, err
	}
	pin := proof.Journal.Origin
	if proof.RequestID != in.RequestId || in.CapacityBytes != uint64(pin.Bytes) {
		return nil, backingIdentityError()
	}
	s := BlockSelection{Provider: d.identity.ProviderID, PoolID: d.identity.PoolID,
		Binding: Binding{AllocationID: "external-" + blockRequestDigest(string(d.uid)+"\x00"+proof.Journal.Owner), CompositeID: in.CompositeVolumeId,
			ResourceID: pin.Handle, DeviceGeneration: 1, CapacityBytes: pin.Bytes},
		Owner: Owner{NodeID: d.identity.Backing.NodeName, Epoch: 1}, PV: pin.PVName, PVUID: string(pin.PVUID),
		Namespace: d.identity.Backing.Namespace, PVC: pin.Name, PVCUID: string(pin.UID), VMID: proof.VMID}
	if observe == nil {
		return nil, status.Error(codes.Unavailable, "independent Azure observer required")
	}
	disk, vm, err := observe(ctx, s)
	if err != nil {
		return nil, err
	}
	if disk == nil || disk.ID == nil || *disk.ID != s.ResourceID || disk.Properties == nil || disk.Properties.UniqueID == nil || *disk.Properties.UniqueID == "" ||
		disk.Properties.DiskSizeBytes == nil || *disk.Properties.DiskSizeBytes != pin.Bytes || disk.Properties.MaxShares == nil || *disk.Properties.MaxShares != 1 ||
		len(disk.ManagedByExtended) != 0 || len(disk.Properties.ShareInfo) != 0 || disk.Properties.ProvisioningState == nil || *disk.Properties.ProvisioningState != "Succeeded" ||
		disk.Properties.PropertyUpdatesInProgress != nil || disk.Properties.DiskState == nil || !strings.EqualFold(vm.ID, proof.VMID) || vm.InstanceGUID == "" || vm.ProvisioningState != "Succeeded" {
		return nil, status.Error(codes.FailedPrecondition, "stable exclusive Azure disk/VM identity required")
	}
	s.DiskUniqueID, s.VMID, s.VMGUID = *disk.Properties.UniqueID, vm.ID, vm.InstanceGUID
	present, err := vm.hasDisk(s.ResourceID)
	if err != nil {
		return nil, err
	}
	phase := "selected"
	switch *disk.Properties.DiskState {
	case armcompute.DiskStateUnattached:
		if disk.ManagedBy != nil || present {
			return nil, status.Error(codes.FailedPrecondition, "attachment views disagree")
		}
	case armcompute.DiskStateAttached:
		if !present {
			return nil, status.Error(codes.FailedPrecondition, "disk absent from selected VM")
		}
		if err := verifyAttachment(s.Binding, Attachment{Disk: disk, VMResourceID: vm.ID, Mode: azureconstants.AttachModeControllerDriven}); err != nil {
			return nil, err
		}
		phase = "published"
	default:
		return nil, status.Error(codes.FailedPrecondition, "disk attachment in transition or foreign use")
	}
	again, err := d.resolveBacking(ctx, in.CompositeVolumeId)
	if err != nil || again != proof {
		return nil, status.Error(codes.Aborted, "external source pins changed during hold admission")
	}
	data, err := InitialBlockHold(s)
	if err != nil {
		return nil, err
	}
	next := cm.DeepCopy()
	for k, v := range data {
		next.Data[k] = v
	}
	next.Data["phase"] = phase
	r.Selection, r.Backing, r.CompositeID, r.Owner = &s, &proof.Journal, in.CompositeVolumeId, proof.Journal.Owner
	r.ReserveID, r.ReserveInput = in.RequestId, digest
	if _, err := d.save(ctx, next, r); err != nil {
		return nil, err
	}
	return d.reservedLease(r), nil
}
func (d *OnlineDomain) reservedLease(r onlineRecord) *pb.Lease {
	b := &publishedRPC{h: d.hold(r)}
	return &pb.Lease{Allocation: b.allocation(), PoolId: d.identity.PoolID, CapacityBytes: uint64(r.Selection.CapacityBytes), State: pb.LeaseState_LEASE_STATE_RESERVED,
		HoldPolicy: pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE}
}
