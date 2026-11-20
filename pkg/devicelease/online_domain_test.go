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
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

type onlineDynamic struct {
	dynamic.Interface
	mu      sync.Mutex
	objects map[string]*unstructured.Unstructured
}
type onlineResource struct {
	dynamic.NamespaceableResourceInterface
	client *onlineDynamic
	gvr    schema.GroupVersionResource
}

func (c *onlineDynamic) Resource(g schema.GroupVersionResource) dynamic.NamespaceableResourceInterface {
	return &onlineResource{client: c, gvr: g}
}
func (r *onlineResource) Get(_ context.Context, name string, _ metav1.GetOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.client.mu.Lock()
	defer r.client.mu.Unlock()
	o := r.client.objects[r.gvr.String()+"/"+name]
	if o == nil {
		return nil, errors.New("source object absent")
	}
	return o.DeepCopy(), nil
}
func (r *onlineResource) Update(_ context.Context, o *unstructured.Unstructured, _ metav1.UpdateOptions, _ ...string) (*unstructured.Unstructured, error) {
	r.client.mu.Lock()
	defer r.client.mu.Unlock()
	key := r.gvr.String() + "/" + o.GetName()
	old := r.client.objects[key]
	if old == nil {
		return nil, errors.New("source object absent")
	}
	n, _ := strconv.Atoi(old.GetResourceVersion())
	copy := o.DeepCopy()
	copy.SetResourceVersion(strconv.Itoa(n + 1))
	r.client.objects[key] = copy
	return copy.DeepCopy(), nil
}

type onlineFixture struct {
	f    *reservationFixture
	d    *OnlineDomain
	dyn  *onlineDynamic
	cv   *unstructured.Unstructured
	j    backingJournal
	req  *pb.ReserveRequest
	disk *armcompute.Disk
	vm   TargetVM
	ca   tls.Certificate
}

func newOnlineFixture(t *testing.T, drivers ...string) *onlineFixture {
	t.Helper()
	f := fixture(t, "pin")
	cfg := OnlineIdentity{ProviderID: "azure-canary-origin", PoolID: "external-block", CompositeCRDUID: "crd-uid",
		Backing: BackingConfig{Namespace: "sources", NamespaceUID: "ns-uid", NodeName: "vm1", NodeUID: "node-uid"}}
	if len(drivers) > 0 {
		require.Len(t, drivers, 2)
		cfg.Backing.OriginDriver, cfg.Backing.CacheDriver = drivers[0], drivers[1]
	}
	id := "dmstack-" + strings.Repeat("a", 52)
	j := backingJournal{Version: 1, Owner: id + ".composite-uid", Config: externalScope{Namespace: "sources", NamespaceUID: "ns-uid", NodeName: "vm1", NodeUID: "node-uid",
		OriginDriver: cfg.Backing.OriginDriver, CacheDriver: cfg.Backing.CacheDriver}, Outer: outerClaim{Name: "application", UID: "outer-uid"},
		Origin: backingClaim{Name: "declared-origin", UID: "origin-pvc-uid", PVName: "origin-pv", PVUID: "origin-pv-uid",
			ClassName: "azure-block", ClassUID: "origin-sc-uid", Driver: cfg.Backing.EffectiveOriginDriver(), Handle: resourceID, Bytes: gib},
		Cache: backingClaim{Name: "declared-cache", UID: "cache-pvc-uid", PVName: "cache-pv", PVUID: "cache-pv-uid",
			ClassName: "acs-block", ClassUID: "cache-sc-uid", Driver: cfg.Backing.EffectiveCacheDriver(), Handle: "vg#pv", Bytes: gib}}
	raw, err := json.Marshal(j)
	require.NoError(t, err)
	cv := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "storage.infinitydb.io/v1alpha1", "kind": "CompositeVolume",
		"metadata": map[string]any{"name": id, "uid": "composite-uid", "resourceVersion": "1",
			"annotations": map[string]any{backingJournalKey: string(raw), backingPrefix + "selected-node": "vm1"}},
		"spec": map[string]any{"capacity": int64(gib), "cacheBytes": int64(gib), "assemblyMode": "host",
			"originParameters": map[string]any{"provider": cfg.ProviderID, "poolID": cfg.PoolID, "requestID": "reserve-origin"}}}}
	crd := &unstructured.Unstructured{Object: map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": "compositevolumes.storage.infinitydb.io", "uid": "crd-uid"},
		"spec": map[string]any{"scope": "Cluster", "group": "storage.infinitydb.io", "names": map[string]any{"plural": "compositevolumes", "kind": "CompositeVolume"},
			"versions": []any{map[string]any{"name": "v1alpha1", "served": true}}}}}
	dyn := &onlineDynamic{objects: map[string]*unstructured.Unstructured{
		compositeGVR.String() + "/" + id: cv.DeepCopy(),
		(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).String() + "/" + crd.GetName(): crd}}
	require.NoError(t, f.client.Tracker().Add(&core.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sources", UID: "ns-uid", ResourceVersion: "1"}}))
	vm := TargetVM{ID: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm1", InstanceGUID: "vm-guid", ProvisioningState: "Succeeded"}
	require.NoError(t, f.client.Tracker().Add(&core.Node{ObjectMeta: metav1.ObjectMeta{Name: "vm1", UID: "node-uid", ResourceVersion: "1"}, Spec: core.NodeSpec{ProviderID: "azure://" + vm.ID}}))
	require.NoError(t, f.client.Tracker().Add(&core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: j.Outer.Name, Namespace: "sources", UID: j.Outer.UID, ResourceVersion: "1",
		Annotations: map[string]string{backingPrefix + "origin-pvc": j.Origin.Name, backingPrefix + "cache-pvc": j.Cache.Name}},
		Spec: core.PersistentVolumeClaimSpec{AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce}, Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}}}))
	for _, pin := range []backingClaim{j.Origin, j.Cache} {
		require.NoError(t, f.client.Tracker().Add(&storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: pin.ClassName, UID: pin.ClassUID, ResourceVersion: "1"}, Provisioner: pin.Driver}))
		require.NoError(t, f.client.Tracker().Add(&core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: pin.Name, Namespace: "sources", UID: pin.UID, ResourceVersion: "1"},
			Spec: core.PersistentVolumeClaimSpec{VolumeName: pin.PVName, StorageClassName: &pin.ClassName, VolumeMode: ptr.To(core.PersistentVolumeBlock), AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}},
			Status: core.PersistentVolumeClaimStatus{Phase: core.ClaimBound, Capacity: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}}))
		require.NoError(t, f.client.Tracker().Add(&core.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: pin.PVName, UID: pin.PVUID, ResourceVersion: "1"},
			Spec: core.PersistentVolumeSpec{StorageClassName: pin.ClassName, VolumeMode: ptr.To(core.PersistentVolumeBlock), AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Capacity: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}, ClaimRef: &core.ObjectReference{Name: pin.Name, Namespace: "sources", UID: pin.UID},
				PersistentVolumeSource: core.PersistentVolumeSource{CSI: &core.CSIPersistentVolumeSource{Driver: pin.Driver, VolumeHandle: pin.Handle}},
				NodeAffinity:           &core.VolumeNodeAffinity{Required: &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchFields: []core.NodeSelectorRequirement{{Key: "metadata.name", Operator: core.NodeSelectorOpIn, Values: []string{"vm1"}}}}}}}},
			Status: core.PersistentVolumeStatus{Phase: core.VolumeBound}}))
	}
	data, err := InitialOnlineDomain(cfg)
	require.NoError(t, err)
	_, err = f.client.CoreV1().ConfigMaps("ns").Create(t.Context(), &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "online", Namespace: "ns", UID: "online-uid", ResourceVersion: "1"}, Data: data}, metav1.CreateOptions{})
	require.NoError(t, err)
	d, err := NewOnlineDomain(cfg, f.client.CoreV1().ConfigMaps("ns"), "online", "online-uid", f.client, dyn)
	require.NoError(t, err)
	return &onlineFixture{f: f, d: d, dyn: dyn, cv: cv, j: j, req: &pb.ReserveRequest{RequestId: "reserve-origin", CompositeVolumeId: id, Pool: &pb.Pool{Provider: cfg.ProviderID, PoolId: cfg.PoolID}, CapacityBytes: uint64(gib)},
		vm: vm, disk: &armcompute.Disk{ID: ptr.To(resourceID), Properties: &armcompute.DiskProperties{UniqueID: ptr.To("existing-guid"), DiskSizeBytes: ptr.To(int64(gib)), MaxShares: ptr.To[int32](1),
			ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateUnattached), CreationData: &armcompute.CreationData{CreateOption: ptr.To(armcompute.DiskCreateOptionImport)}}}, ca: testCert(t, 1, nil, false)}
}

func (f *onlineFixture) connect(t *testing.T, role ReservationRole) pb.DeviceProviderClient {
	t.Helper()
	cert := testCert(t, 2, &f.ca, false)
	digest := sha256.Sum256(cert.Certificate[0])
	roots := x509.NewCertPool()
	roots.AddCert(f.ca.Leaf)
	node := ""
	if role == ReservationNode {
		node = "vm1"
	}
	server, err := NewOnlineServer(f.d, func(context.Context, BlockSelection) (*armcompute.Disk, TargetVM, error) { return f.disk, f.vm, nil },
		testCert(t, 3, &f.ca, true), roots, []OnlineGrant{{hex.EncodeToString(digest[:]), role, node}})
	require.NoError(t, err)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "origin.provider.example", Certificates: []tls.Certificate{cert}})))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewDeviceProviderClient(conn)
}
func (f *onlineFixture) restart(t *testing.T) {
	t.Helper()
	var err error
	f.d, err = NewOnlineDomain(f.d.identity, f.d.maps, f.d.name, f.d.uid, f.f.client, f.dyn)
	require.NoError(t, err)
}
func (f *onlineFixture) attach() {
	f.disk.ManagedBy = &f.vm.ID
	f.disk.Properties.DiskState = ptr.To(armcompute.DiskStateAttached)
	f.vm.DataDisks = []*armcompute.DataDisk{{ManagedDisk: &armcompute.ManagedDiskParameters{ID: ptr.To(resourceID)}, Lun: ptr.To[int32](0)}}
}

func TestOnlineExternalReserveAndClosureNeverOwnSources(t *testing.T) {
	f := newOnlineFixture(t)
	require.NoError(t, f.d.Check(t.Context()))
	c := f.connect(t, ReservationController)
	f.f.client.ClearActions()
	l, err := c.Reserve(t.Context(), f.req)
	require.NoError(t, err)
	require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, l.State)
	require.Equal(t, "existing-guid", l.Allocation.StableDeviceId)
	require.Equal(t, resourceID, l.Allocation.ResourceId)
	require.NoError(t, f.d.AllowNativeRead(t.Context(), resourceID+"-other"))
	require.Equal(t, codes.FailedPrecondition, status.Code(f.d.AllowNativeRead(t.Context(), resourceID)))
	// No native-create history or Empty tag exists, and none is required.
	_, r, err := f.d.read(t.Context())
	require.NoError(t, err)
	require.Equal(t, "declared-origin", r.Selection.PVC)
	_, _, err = f.d.BeginNative(t.Context(), "DeleteVolume", "user-delete-source", resourceID, "", "")
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	// Unrelated ordinary traffic remains admitted in this separate driver.
	ctx, _, err := f.d.BeginNative(t.Context(), "DeleteVolume", "other", resourceID+"-other", "", "")
	require.NoError(t, err)
	require.NoError(t, f.d.EndNative(ctx, true, "", nil))
	_, err = c.Release(t.Context(), &pb.ReleaseRequest{RequestId: "close", Allocation: l.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION})
	require.NoError(t, err)
	_, err = c.GetLease(t.Context(), l.Allocation.Key)
	require.Equal(t, codes.NotFound, status.Code(err))
	for _, a := range f.f.client.Actions() {
		if a.GetVerb() != "get" {
			require.Equal(t, "update", a.GetVerb())
			require.Equal(t, "configmaps", a.GetResource().Resource)
		}
	}
	_, err = f.f.client.CoreV1().PersistentVolumeClaims("sources").Get(t.Context(), f.j.Origin.Name, metav1.GetOptions{})
	require.NoError(t, err)
	_, err = f.f.client.CoreV1().PersistentVolumes().Get(t.Context(), f.j.Origin.PVName, metav1.GetOptions{})
	require.NoError(t, err)
}

func TestOnlineNativePublicationThenRetainedOwnerSurvivesMetadataDeletion(t *testing.T) {
	f := newOnlineFixture(t)
	controller := f.connect(t, ReservationController)
	l, err := controller.Reserve(t.Context(), f.req)
	require.NoError(t, err)
	ctx, h, err := f.d.BeginNative(t.Context(), "ControllerPublishVolume", "publish", resourceID, "", "")
	require.NoError(t, err)
	done, err := h.BeginSetup(ctx, true)
	require.NoError(t, err)
	f.attach()
	require.NoError(t, done())
	require.NoError(t, f.d.EndNative(ctx, true, "", nil))
	node := f.connect(t, ReservationNode)
	owner := &pb.Owner{Allocation: l.Allocation, NodeId: "vm1", OwnerEpoch: 1}
	acquire := &pb.AcquireRequest{RequestId: "acquire", Owner: owner}
	_, err = node.Acquire(t.Context(), acquire)
	require.NoError(t, err)
	require.NoError(t, f.f.client.CoreV1().PersistentVolumeClaims("sources").Delete(t.Context(), f.j.Origin.Name, metav1.DeleteOptions{}))
	require.NoError(t, f.f.client.CoreV1().PersistentVolumes().Delete(t.Context(), f.j.Origin.PVName, metav1.DeleteOptions{}))
	require.NoError(t, f.f.client.CoreV1().Nodes().Delete(t.Context(), "vm1", metav1.DeleteOptions{}))
	f.dyn.mu.Lock()
	delete(f.dyn.objects, compositeGVR.String()+"/"+f.req.CompositeVolumeId)
	f.dyn.mu.Unlock()
	f.restart(t)
	require.NoError(t, f.d.Check(t.Context()))
	node = f.connect(t, ReservationNode)
	controller = f.connect(t, ReservationController)
	_, err = node.Acquire(t.Context(), acquire)
	require.NoError(t, err)
	_, err = node.Renew(t.Context(), &pb.RenewRequest{RequestId: "renew", Owner: owner})
	require.NoError(t, err)
	_, err = controller.Release(t.Context(), &pb.ReleaseRequest{RequestId: "too-early", Allocation: l.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION, OwnerEpoch: 1})
	require.Equal(t, codes.FailedPrecondition, status.Code(err), "absence never releases held ownership")
	_, r, err := f.d.read(t.Context())
	require.NoError(t, err)
	held := f.d.hold(r)
	held.cleanup = blockReceipt{*r.Selection}
	require.NoError(t, held.release(t.Context(), "verified-node-release", r.Selection.Binding, r.Selection.Owner, "verified"))
	// Node-use teardown is not final allocation closure.
	ctx, _, err = f.d.BeginNative(t.Context(), "ControllerUnpublishVolume", "unpublish", resourceID, "", "")
	require.NoError(t, err)
	require.NoError(t, f.d.EndNative(ctx, true, "", nil))
	_, r, err = f.d.read(t.Context())
	require.NoError(t, err)
	require.False(t, r.Closed)
	current, err := controller.GetLease(t.Context(), l.Allocation.Key)
	require.NoError(t, err)
	require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, current.State)
	require.Equal(t, uint64(1), current.OwnerEpoch)
	newOwner := proto.CloneOf(owner)
	newOwner.OwnerEpoch = 2
	_, err = node.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "restart-epoch", Owner: newOwner})
	require.Equal(t, codes.Aborted, status.Code(err), "higher-epoch reuse is not implemented")
	_, err = controller.Release(t.Context(), &pb.ReleaseRequest{RequestId: "close", Allocation: l.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION, OwnerEpoch: 1})
	require.NoError(t, err)
}

func TestOnlineExternalPinDriftAndLegacyJournalFailWithoutEffects(t *testing.T) {
	for _, kind := range []string{"outer-ref", "outer-uid", "source-uid", "source-driver", "source-size", "legacy", "empty-owner", "same-source"} {
		t.Run(kind, func(t *testing.T) {
			f := newOnlineFixture(t)
			switch kind {
			case "outer-ref", "outer-uid":
				p, err := f.f.client.CoreV1().PersistentVolumeClaims("sources").Get(t.Context(), "application", metav1.GetOptions{})
				require.NoError(t, err)
				if kind == "outer-ref" {
					p.Annotations[backingPrefix+"origin-pvc"] = "other"
				} else {
					p.UID = "replaced"
				}
				_, err = f.f.client.CoreV1().PersistentVolumeClaims("sources").Update(t.Context(), p, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "source-uid":
				p, err := f.f.client.CoreV1().PersistentVolumeClaims("sources").Get(t.Context(), f.j.Origin.Name, metav1.GetOptions{})
				require.NoError(t, err)
				p.UID = "replacement"
				_, err = f.f.client.CoreV1().PersistentVolumeClaims("sources").Update(t.Context(), p, metav1.UpdateOptions{})
				require.NoError(t, err)
			case "source-driver", "source-size":
				p, err := f.f.client.CoreV1().PersistentVolumes().Get(t.Context(), f.j.Origin.PVName, metav1.GetOptions{})
				require.NoError(t, err)
				if kind == "source-driver" {
					p.Spec.CSI.Driver = "foreign.example"
				} else {
					p.Spec.Capacity[core.ResourceStorage] = resource.MustParse("2Gi")
				}
				_, err = f.f.client.CoreV1().PersistentVolumes().Update(t.Context(), p, metav1.UpdateOptions{})
				require.NoError(t, err)
			default:
				a := f.cv.GetAnnotations()
				if kind == "legacy" {
					a[legacyBackingJournalKey] = a[backingJournalKey]
				} else {
					if kind == "empty-owner" {
						f.j.Owner = ""
					} else {
						f.j.Cache.Name = f.j.Origin.Name
					}
					b, err := json.Marshal(f.j)
					require.NoError(t, err)
					a[backingJournalKey] = string(b)
				}
				f.cv.SetAnnotations(a)
				_, err := f.dyn.Resource(compositeGVR).Update(t.Context(), f.cv, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			before, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			_, err = f.connect(t, ReservationController).Reserve(t.Context(), f.req)
			require.Error(t, err)
			after, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, after)
		})
	}
}

func TestOnlineLostReserveAndClosureReplies(t *testing.T) {
	f := newOnlineFixture(t)
	loss := true
	f.f.client.PrependReactor("update", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		if !loss {
			return false, nil, nil
		}
		loss = false
		next := a.(ktesting.UpdateAction).GetObject().(*core.ConfigMap).DeepCopy()
		n, err := strconv.Atoi(next.ResourceVersion)
		require.NoError(t, err)
		next.ResourceVersion = strconv.Itoa(n + 1)
		require.NoError(t, f.f.client.Tracker().Update(a.GetResource(), next, next.Namespace))
		return true, nil, errUncertain
	})
	c := f.connect(t, ReservationController)
	_, err := c.Reserve(t.Context(), f.req)
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.cv.SetDeletionTimestamp(ptr.To(metav1.Now()))
	_, err = f.dyn.Resource(compositeGVR).Update(t.Context(), f.cv, metav1.UpdateOptions{})
	require.NoError(t, err)
	f.restart(t)
	c = f.connect(t, ReservationController)
	l, err := c.Reserve(t.Context(), f.req)
	require.NoError(t, err)
	loss = true
	close := &pb.ReleaseRequest{RequestId: "close", Allocation: l.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION}
	_, err = c.Release(t.Context(), close)
	require.Equal(t, codes.Unavailable, status.Code(err))
	f.restart(t)
	c = f.connect(t, ReservationController)
	_, err = c.Release(t.Context(), close)
	require.NoError(t, err)
	_, err = c.GetLease(t.Context(), l.Allocation.Key)
	require.Equal(t, codes.NotFound, status.Code(err))
}
