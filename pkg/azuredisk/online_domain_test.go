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

package azuredisk

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	mockvm "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
)

type onlineTestScope struct {
	Namespace    string    `json:"namespace"`
	NamespaceUID types.UID `json:"namespaceUID"`
	NodeName     string    `json:"nodeName"`
	NodeUID      types.UID `json:"nodeUID"`
	OriginDriver string    `json:"originDriver,omitempty"`
	CacheDriver  string    `json:"cacheDriver,omitempty"`
}
type onlineTestOuter struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}
type onlineTestClaim struct {
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
	PVName    string    `json:"pvName"`
	PVUID     types.UID `json:"pvUID"`
	ClassName string    `json:"className"`
	ClassUID  types.UID `json:"classUID"`
	Driver    string    `json:"driver"`
	Handle    string    `json:"handle"`
	Bytes     int64     `json:"bytes"`
}
type onlineTestJournal struct {
	Version int             `json:"version"`
	Owner   string          `json:"owner"`
	Config  onlineTestScope `json:"config"`
	Outer   onlineTestOuter `json:"outer"`
	Origin  onlineTestClaim `json:"origin"`
	Cache   onlineTestClaim `json:"cache"`
}

func onlineAddress(t *testing.T) string {
	t.Helper()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, e)
	a := l.Addr().String()
	require.NoError(t, l.Close())
	return a
}
func onlineCertificate(t *testing.T, serial int64, parent *tls.Certificate, server bool) tls.Certificate {
	t.Helper()
	key, e := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, e)
	template := &x509.Certificate{SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: "online-test"}, NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, BasicConstraintsValid: true}
	signer := any(key)
	issuer := template
	if parent == nil {
		template.IsCA = true
		template.KeyUsage |= x509.KeyUsageCertSign
	} else {
		issuer, signer = parent.Leaf, parent.PrivateKey
		if server {
			template.DNSNames = []string{"online.provider.test"}
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
		} else {
			template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
		}
	}
	der, e := x509.CreateCertificate(rand.Reader, template, issuer, &key.PublicKey, signer)
	require.NoError(t, e)
	leaf, e := x509.ParseCertificate(der)
	require.NoError(t, e)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}
}

type onlineDriverFixture struct {
	d                            *Driver
	kube                         *fake.Clientset
	cfg                          onlineDomainConfig
	journal                      onlineTestJournal
	composite                    map[string]any
	sourceMu                     sync.Mutex
	id, diskID, vmID             string
	controllerCert, nodeCert, ca tls.Certificate
}

func newOnlineDriverFixture(t *testing.T, drivers ...string) *onlineDriverFixture {
	t.Helper()
	driver, err := NewFakeDriver(gomock.NewController(t))
	require.NoError(t, err)
	d := &driver.(*fakeDriver).Driver
	d.NodeID = ""
	d.onlineConfigPath = filepath.Join(t.TempDir(), "online.json")
	d.endpoint = "tcp://" + onlineAddress(t)
	diskID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/existing-origin", d.cloud.SubscriptionID, d.cloud.ResourceGroup)
	vmID := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/vm1", d.cloud.SubscriptionID, d.cloud.ResourceGroup)
	id := "dmstack-" + strings.Repeat("a", 52)
	cfg := onlineDomainConfig{Identity: devicelease.OnlineIdentity{ProviderID: "azure-canary-origin", PoolID: "external-block", CompositeCRDUID: "crd-uid",
		Backing: devicelease.BackingConfig{Namespace: "sources", NamespaceUID: "ns-uid", NodeName: "vm1", NodeUID: "node-uid"}},
		JournalNamespace: "ns", JournalName: "online", JournalUID: "online-uid", Endpoint: onlineAddress(t)}
	if len(drivers) > 0 {
		require.Len(t, drivers, 2)
		cfg.Identity.Backing.OriginDriver, cfg.Identity.Backing.CacheDriver = drivers[0], drivers[1]
		d.Name = cfg.Identity.Backing.EffectiveOriginDriver()
	}
	j := onlineTestJournal{Version: 1, Owner: id + ".composite-uid", Config: onlineTestScope{Namespace: "sources", NamespaceUID: "ns-uid", NodeName: "vm1", NodeUID: "node-uid",
		OriginDriver: cfg.Identity.Backing.OriginDriver, CacheDriver: cfg.Identity.Backing.CacheDriver}, Outer: onlineTestOuter{Name: "application", UID: "outer-uid"},
		Origin: onlineTestClaim{Name: "my-origin", UID: "origin-pvc-uid", PVName: "origin-pv", PVUID: "origin-pv-uid", ClassName: "azure-block", ClassUID: "origin-sc-uid", Driver: cfg.Identity.Backing.EffectiveOriginDriver(), Handle: diskID, Bytes: 1 << 30},
		Cache:  onlineTestClaim{Name: "my-cache", UID: "cache-pvc-uid", PVName: "cache-pv", PVUID: "cache-pv-uid", ClassName: "acs-block", ClassUID: "cache-sc-uid", Driver: cfg.Identity.Backing.EffectiveCacheDriver(), Handle: "vg#pv", Bytes: 1 << 30}}
	raw, err := json.Marshal(j)
	require.NoError(t, err)
	cv := map[string]any{"apiVersion": "storage.infinitydb.io/v1alpha1", "kind": "CompositeVolume", "metadata": map[string]any{"name": id, "uid": "composite-uid", "resourceVersion": "1",
		"annotations": map[string]any{"dmstack.csi.infinitydb.io/external-source-pvcs-v1": string(raw), "dmstack.csi.infinitydb.io/selected-node": "vm1"}},
		"spec": map[string]any{"capacity": int64(1 << 30), "cacheBytes": int64(1 << 30), "assemblyMode": "host",
			"originParameters": map[string]any{"provider": cfg.Identity.ProviderID, "poolID": cfg.Identity.PoolID, "requestID": "reserve-origin"}}}
	initial, err := devicelease.InitialOnlineDomain(cfg.Identity)
	require.NoError(t, err)
	kube := fake.NewSimpleClientset(&core.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "online", Namespace: "ns", UID: "online-uid", ResourceVersion: "1"}, Data: initial},
		&core.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "sources", UID: "ns-uid", ResourceVersion: "1"}},
		&core.Node{ObjectMeta: metav1.ObjectMeta{Name: "vm1", UID: "node-uid", ResourceVersion: "1"}, Spec: core.NodeSpec{ProviderID: "azure://" + vmID}})
	kube.PrependReactor("update", "configmaps", func(a ktesting.Action) (bool, runtime.Object, error) {
		next := a.(ktesting.UpdateAction).GetObject().(*core.ConfigMap).DeepCopy()
		old, err := kube.Tracker().Get(a.GetResource(), next.Namespace, next.Name)
		if err != nil {
			return true, nil, err
		}
		prev := old.(*core.ConfigMap)
		if next.UID != prev.UID || next.ResourceVersion != prev.ResourceVersion {
			return true, nil, apierrors.NewConflict(a.GetResource().GroupResource(), next.Name, errors.New("stale RV"))
		}
		n, e := strconv.Atoi(next.ResourceVersion)
		require.NoError(t, e)
		next.ResourceVersion = strconv.Itoa(n + 1)
		require.NoError(t, kube.Tracker().Update(a.GetResource(), next, next.Namespace))
		return true, next, nil
	})
	require.NoError(t, kube.Tracker().Add(&core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: j.Outer.Name, Namespace: "sources", UID: j.Outer.UID, ResourceVersion: "1",
		Annotations: map[string]string{"dmstack.csi.infinitydb.io/origin-pvc": j.Origin.Name, "dmstack.csi.infinitydb.io/cache-pvc": j.Cache.Name}},
		Spec: core.PersistentVolumeClaimSpec{AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce}, Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}}}))
	for _, p := range []onlineTestClaim{j.Origin, j.Cache} {
		require.NoError(t, kube.Tracker().Add(&storage.StorageClass{ObjectMeta: metav1.ObjectMeta{Name: p.ClassName, UID: p.ClassUID, ResourceVersion: "1"}, Provisioner: p.Driver}))
		require.NoError(t, kube.Tracker().Add(&core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: p.Name, Namespace: "sources", UID: p.UID, ResourceVersion: "1"},
			Spec: core.PersistentVolumeClaimSpec{VolumeName: p.PVName, StorageClassName: &p.ClassName, VolumeMode: ptr.To(core.PersistentVolumeBlock), AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Resources: core.VolumeResourceRequirements{Requests: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}},
			Status: core.PersistentVolumeClaimStatus{Phase: core.ClaimBound, Capacity: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}}}))
		require.NoError(t, kube.Tracker().Add(&core.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: p.PVName, UID: p.PVUID, ResourceVersion: "1"},
			Spec: core.PersistentVolumeSpec{StorageClassName: p.ClassName, VolumeMode: ptr.To(core.PersistentVolumeBlock), AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				Capacity: core.ResourceList{core.ResourceStorage: resource.MustParse("1Gi")}, ClaimRef: &core.ObjectReference{Name: p.Name, Namespace: "sources", UID: p.UID},
				PersistentVolumeSource: core.PersistentVolumeSource{CSI: &core.CSIPersistentVolumeSource{Driver: p.Driver, VolumeHandle: p.Handle}},
				NodeAffinity:           &core.VolumeNodeAffinity{Required: &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{{MatchFields: []core.NodeSelectorRequirement{{Key: "metadata.name", Operator: core.NodeSelectorOpIn, Values: []string{"vm1"}}}}}}}},
			Status: core.PersistentVolumeStatus{Phase: core.VolumeBound}}))
	}
	d.kubeClient = kube
	f := &onlineDriverFixture{d: d, kube: kube, cfg: cfg, journal: j, composite: cv, id: id, diskID: diskID, vmID: vmID}
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("unexpected source write: %s", r.Method)
			w.WriteHeader(405)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/apis/apiextensions.k8s.io/v1/customresourcedefinitions/compositevolumes.storage.infinitydb.io" {
			_ = json.NewEncoder(w).Encode(map[string]any{"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition", "metadata": map[string]any{"name": "compositevolumes.storage.infinitydb.io", "uid": "crd-uid"},
				"spec": map[string]any{"scope": "Cluster", "group": "storage.infinitydb.io", "names": map[string]any{"plural": "compositevolumes", "kind": "CompositeVolume"}, "versions": []any{map[string]any{"name": "v1alpha1", "served": true}}}})
			return
		}
		if r.URL.Path == "/apis/storage.infinitydb.io/v1alpha1/compositevolumes/"+id {
			f.sourceMu.Lock()
			defer f.sourceMu.Unlock()
			_ = json.NewEncoder(w).Encode(f.composite)
			return
		}
		http.NotFound(w, r)
	}))
	t.Cleanup(source.Close)
	d.onlineRESTConfig = &rest.Config{Host: source.URL}
	f.ca = onlineCertificate(t, 1, nil, false)
	f.controllerCert = onlineCertificate(t, 2, &f.ca, false)
	f.nodeCert = onlineCertificate(t, 3, &f.ca, false)
	server := onlineCertificate(t, 4, &f.ca, true)
	dir := filepath.Dir(d.onlineConfigPath)
	f.cfg.ServerCertFile, f.cfg.ServerKeyFile, f.cfg.ClientCAFile = filepath.Join(dir, "server.pem"), filepath.Join(dir, "key.pem"), filepath.Join(dir, "ca.pem")
	key, err := x509.MarshalPKCS8PrivateKey(server.PrivateKey)
	require.NoError(t, err)
	for path, block := range map[string]*pem.Block{f.cfg.ServerCertFile: {Type: "CERTIFICATE", Bytes: server.Certificate[0]}, f.cfg.ServerKeyFile: {Type: "PRIVATE KEY", Bytes: key}, f.cfg.ClientCAFile: {Type: "CERTIFICATE", Bytes: f.ca.Certificate[0]}} {
		require.NoError(t, os.WriteFile(path, pem.EncodeToMemory(block), 0600))
	}
	for _, g := range []struct {
		cert tls.Certificate
		role devicelease.ReservationRole
		node string
	}{{f.controllerCert, devicelease.ReservationController, ""}, {f.nodeCert, devicelease.ReservationNode, "vm1"}} {
		h := sha256.Sum256(g.cert.Certificate[0])
		f.cfg.Grants = append(f.cfg.Grants, devicelease.OnlineGrant{ClientSHA256: hex.EncodeToString(h[:]), Role: g.role, NodeID: g.node})
	}
	b, err := json.Marshal(f.cfg)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(d.onlineConfigPath, b, 0600))
	require.NoError(t, os.Chmod(d.onlineConfigPath, 0444))
	t.Cleanup(func() { _ = os.Chmod(d.onlineConfigPath, 0600) })
	return f
}

func (f *onlineDriverFixture) start(t *testing.T) (csi.ControllerClient, pb.DeviceProviderClient) {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- f.d.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case e := <-done:
			require.NoError(t, e)
		case <-time.After(5 * time.Second):
			t.Error("driver did not stop")
		}
	})
	c, e := grpc.NewClient(strings.TrimPrefix(f.d.endpoint, "tcp://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, e)
	t.Cleanup(func() { _ = c.Close() })
	ready, stop := context.WithTimeout(t.Context(), 5*time.Second)
	defer stop()
	info, e := csi.NewIdentityClient(c).GetPluginInfo(ready, &csi.GetPluginInfoRequest{}, grpc.WaitForReady(true))
	require.NoError(t, e)
	require.Equal(t, f.d.Name, info.Name)
	return csi.NewControllerClient(c), f.provider(t, f.controllerCert)
}
func (f *onlineDriverFixture) provider(t *testing.T, cert tls.Certificate) pb.DeviceProviderClient {
	t.Helper()
	roots := x509.NewCertPool()
	roots.AddCert(f.ca.Leaf)
	c, e := grpc.NewClient(f.cfg.Endpoint, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS13, RootCAs: roots, ServerName: "online.provider.test", Certificates: []tls.Certificate{cert}})))
	require.NoError(t, e)
	t.Cleanup(func() { _ = c.Close() })
	return pb.NewDeviceProviderClient(c)
}

func TestOnlineDriverExternalSourcesReserveWithoutProvisioning(t *testing.T) {
	for _, drivers := range [][2]string{{"", ""}, {"disk.dmstack.infinitydb.io", "localdisk.dmstack.infinitydb.io"}} {
		t.Run(drivers[0], func(t *testing.T) {
			f := newOnlineDriverFixture(t, drivers[0], drivers[1])
			ctrl := gomock.NewController(t)
			dc := mock_diskclient.NewMockInterface(ctrl)
			f.d.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(f.d.cloud.SubscriptionID).Return(dc, nil).AnyTimes()
			disk := &armcompute.Disk{ID: &f.diskID, Properties: &armcompute.DiskProperties{UniqueID: ptr.To("existing-guid"), DiskSizeBytes: ptr.To[int64](1 << 30), MaxShares: ptr.To[int32](1),
				DiskState: ptr.To(armcompute.DiskStateUnattached), ProvisioningState: ptr.To("Succeeded")}}
			dc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "existing-origin").Return(disk, nil).AnyTimes()
			// No CreateOrUpdate or Delete expectation: composite closure must
			// not provision/delete the independently owned source.
			vm := setTestVirtualMachines(f.d.cloud, map[string]string{"vm1": "PowerState/Running"}, false)[0]
			vm.ID = &f.vmID
			vm.Properties.VMID = ptr.To("vm-guid")
			vm.Properties.OSProfile = &armcompute.OSProfile{ComputerName: vm.Name}
			vm.Properties.StorageProfile.DataDisks = nil
			f.d.clientFactory.GetVirtualMachineClient().(*mockvm.MockInterface).EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "vm1", gomock.Any()).Return(&vm, nil).AnyTimes()
			native, provider := f.start(t)
			f.kube.ClearActions()
			q := &pb.ReserveRequest{RequestId: "reserve-origin", CompositeVolumeId: f.id, Pool: &pb.Pool{Provider: f.cfg.Identity.ProviderID, PoolId: f.cfg.Identity.PoolID}, CapacityBytes: 1 << 30}
			lease, e := provider.Reserve(t.Context(), q)
			require.NoError(t, e)
			require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, lease.State)
			require.Equal(t, f.diskID, lease.Allocation.ResourceId)
			_, e = native.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: f.diskID})
			require.Equal(t, codes.FailedPrecondition, status.Code(e))
			other := strings.Replace(f.diskID, "existing-origin", "ordinary-other", 1)
			dc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "ordinary-other").Return(&armcompute.Disk{ID: &other}, nil)
			dc.EXPECT().Delete(gomock.Any(), f.d.cloud.ResourceGroup, "ordinary-other").Return(nil)
			_, e = native.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: other})
			require.NoError(t, e)
			_, e = provider.Release(t.Context(), &pb.ReleaseRequest{RequestId: "close", Allocation: lease.Allocation, Mode: pb.ReleaseMode_RELEASE_MODE_ALLOCATION})
			require.NoError(t, e)
			_, e = provider.GetLease(t.Context(), lease.Allocation.Key)
			require.Equal(t, codes.NotFound, status.Code(e))
			for _, p := range []onlineTestClaim{f.journal.Origin, f.journal.Cache} {
				_, e = f.kube.CoreV1().PersistentVolumeClaims("sources").Get(t.Context(), p.Name, metav1.GetOptions{})
				require.NoError(t, e)
				_, e = f.kube.CoreV1().PersistentVolumes().Get(t.Context(), p.PVName, metav1.GetOptions{})
				require.NoError(t, e)
			}
			for _, a := range f.kube.Actions() {
				if a.GetVerb() != "get" {
					require.Equal(t, "update", a.GetVerb())
					require.Equal(t, "configmaps", a.GetResource().Resource)
				}
			}
		})
	}
}

func TestOnlineDriverGuardedPublicationPrecedesAcquire(t *testing.T) {
	f := newOnlineDriverFixture(t, "disk.dmstack.infinitydb.io", "localdisk.dmstack.infinitydb.io")
	ctrl := gomock.NewController(t)
	dc := mock_diskclient.NewMockInterface(ctrl)
	f.d.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(f.d.cloud.SubscriptionID).Return(dc, nil).AnyTimes()
	var mu sync.Mutex
	disk := &armcompute.Disk{ID: &f.diskID, Properties: &armcompute.DiskProperties{UniqueID: ptr.To("existing-guid"), DiskSizeBytes: ptr.To[int64](1 << 30), DiskSizeGB: ptr.To[int32](1),
		MaxShares: ptr.To[int32](1), ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateUnattached)}}
	dc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "existing-origin").DoAndReturn(func(context.Context, string, string) (*armcompute.Disk, error) {
		mu.Lock()
		defer mu.Unlock()
		return disk, nil
	}).AnyTimes()
	vm := setTestVirtualMachines(f.d.cloud, map[string]string{"vm1": "PowerState/Running"}, false)[0]
	vm.ID = &f.vmID
	vm.Properties.VMID = ptr.To("vm-guid")
	vm.Properties.OSProfile = &armcompute.OSProfile{ComputerName: vm.Name}
	vm.Properties.StorageProfile.DataDisks = nil
	vc := f.d.clientFactory.GetVirtualMachineClient().(*mockvm.MockInterface)
	vc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "vm1", gomock.Any()).Return(&vm, nil).AnyTimes()
	vc.EXPECT().CreateOrUpdate(gomock.Any(), f.d.cloud.ResourceGroup, "vm1", gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, updated armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
		mu.Lock()
		defer mu.Unlock()
		vm.Properties.StorageProfile = updated.Properties.StorageProfile
		disk.ManagedBy = &f.vmID
		disk.Properties.DiskState = ptr.To(armcompute.DiskStateAttached)
		return &vm, nil
	}).Times(1)
	native, provider := f.start(t)
	l, e := provider.Reserve(t.Context(), &pb.ReserveRequest{RequestId: "reserve-origin", CompositeVolumeId: f.id, Pool: &pb.Pool{Provider: f.cfg.Identity.ProviderID, PoolId: f.cfg.Identity.PoolID}, CapacityBytes: 1 << 30})
	require.NoError(t, e)
	node := f.provider(t, f.nodeCert)
	a := &pb.AcquireRequest{RequestId: "acquire", Owner: &pb.Owner{Allocation: l.Allocation, NodeId: "vm1", OwnerEpoch: 1}}
	_, e = node.Acquire(t.Context(), a)
	require.Equal(t, codes.FailedPrecondition, status.Code(e))
	publish := &csi.ControllerPublishVolumeRequest{VolumeId: f.diskID, NodeId: "vm1", VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}
	_, e = native.ControllerPublishVolume(t.Context(), publish)
	require.NoError(t, e)
	held, e := node.Acquire(t.Context(), a)
	require.NoError(t, e)
	require.Equal(t, pb.LeaseState_LEASE_STATE_ACQUIRED, held.State)
	require.Equal(t, pb.HoldPolicy_HOLD_POLICY_EXPLICIT_RELEASE, held.HoldPolicy)
	require.Zero(t, held.RenewByUnixSeconds)
	require.NoError(t, f.kube.CoreV1().PersistentVolumeClaims("sources").Delete(t.Context(), f.journal.Origin.Name, metav1.DeleteOptions{}))
	require.NoError(t, f.kube.CoreV1().PersistentVolumes().Delete(t.Context(), f.journal.Origin.PVName, metav1.DeleteOptions{}))
	again, e := provider.GetLease(t.Context(), l.Allocation.Key)
	require.NoError(t, e)
	require.True(t, proto.Equal(held, again))
	_, e = node.Renew(t.Context(), &pb.RenewRequest{RequestId: "renew", Owner: a.Owner})
	require.Equal(t, codes.FailedPrecondition, status.Code(e))
	_, e = native.ControllerUnpublishVolume(t.Context(), &csi.ControllerUnpublishVolumeRequest{VolumeId: f.diskID, NodeId: "vm1"})
	require.Equal(t, codes.FailedPrecondition, status.Code(e))
}

func TestOnlineStartupRejectsMissingJournalAndWrongNode(t *testing.T) {
	for _, mode := range []string{"missing-journal", "wrong-node", "node-role", "bad-config"} {
		t.Run(mode, func(t *testing.T) {
			f := newOnlineDriverFixture(t)
			switch mode {
			case "missing-journal":
				require.NoError(t, f.kube.CoreV1().ConfigMaps("ns").Delete(t.Context(), "online", metav1.DeleteOptions{}))
			case "wrong-node":
				f.d.NodeID = "other-node"
			case "node-role":
				f.d.NodeID = "vm1"
			case "bad-config":
				require.NoError(t, os.Chmod(f.d.onlineConfigPath, 0600))
				require.NoError(t, os.WriteFile(f.d.onlineConfigPath, []byte(`{"unknown":true}`), 0600))
				require.NoError(t, os.Chmod(f.d.onlineConfigPath, 0444))
			}
			cfg, e := f.d.configureOnline(t.Context())
			if mode == "node-role" {
				require.NoError(t, e)
				require.NotNil(t, f.d.onlineDomain)
				s := grpc.NewServer()
				stop, e := f.d.startOnline(t.Context(), cfg, s)
				require.NoError(t, e)
				stop()
				l, e := net.Listen("tcp", f.cfg.Endpoint)
				require.NoError(t, e)
				require.NoError(t, l.Close())
			} else {
				require.Error(t, e)
				require.Nil(t, f.d.onlineDomain)
			}
		})
	}
}
