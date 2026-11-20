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
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	mockvm "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
)

type completionClient struct {
	kubernetes.Interface
	core coreclient.CoreV1Interface
}

func (c completionClient) CoreV1() coreclient.CoreV1Interface { return c.core }

type completionCore struct {
	coreclient.CoreV1Interface
	maps coreclient.ConfigMapInterface
}

func (c completionCore) ConfigMaps(ns string) coreclient.ConfigMapInterface {
	if ns == "ns" {
		return c.maps
	}
	return c.CoreV1Interface.ConfigMaps(ns)
}

type completionMapClient struct {
	coreclient.ConfigMapInterface
	before func(context.Context, *core.ConfigMap)
}

func (c *completionMapClient) Update(ctx context.Context, cm *core.ConfigMap, o metav1.UpdateOptions) (*core.ConfigMap, error) {
	c.before(ctx, cm)
	return c.ConfigMapInterface.Update(ctx, cm, o)
}

func TestOnlineNativeCompletionRetriesCASWithoutRepeatingHandler(t *testing.T) {
	f := newOnlineDriverFixture(t, "disk.dmstack.infinitydb.io", "localdisk.dmstack.infinitydb.io")
	ctrl := gomock.NewController(t)
	dc := mock_diskclient.NewMockInterface(ctrl)
	f.d.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(f.d.cloud.SubscriptionID).Return(dc, nil).AnyTimes()
	disk := &armcompute.Disk{ID: &f.diskID, ManagedBy: &f.vmID, Properties: &armcompute.DiskProperties{UniqueID: ptr.To("existing-guid"), DiskSizeBytes: ptr.To[int64](1 << 30),
		MaxShares: ptr.To[int32](1), ProvisioningState: ptr.To("Succeeded"), DiskState: ptr.To(armcompute.DiskStateAttached)}}
	dc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "existing-origin").Return(disk, nil).AnyTimes()
	vm := setTestVirtualMachines(f.d.cloud, map[string]string{"vm1": "PowerState/Running"}, false)[0]
	vm.ID = &f.vmID
	vm.Properties.VMID = ptr.To("vm-guid")
	vm.Properties.OSProfile = &armcompute.OSProfile{ComputerName: vm.Name}
	vm.Properties.StorageProfile.DataDisks = []*armcompute.DataDisk{{ManagedDisk: &armcompute.ManagedDiskParameters{ID: &f.diskID}, Lun: ptr.To[int32](0)}}
	f.d.clientFactory.GetVirtualMachineClient().(*mockvm.MockInterface).EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "vm1", gomock.Any()).Return(&vm, nil).AnyTimes()
	other := strings.Replace(f.diskID, "existing-origin", "ordinary-other", 1)
	var effects atomic.Int32
	dc.EXPECT().Get(gomock.Any(), f.d.cloud.ResourceGroup, "ordinary-other").Return(&armcompute.Disk{ID: &other}, nil).Times(1)
	dc.EXPECT().Delete(gomock.Any(), f.d.cloud.ResourceGroup, "ordinary-other").DoAndReturn(func(context.Context, string, string) error {
		effects.Add(1)
		return nil
	}).Times(1)
	base := f.kube.CoreV1().ConfigMaps("ns")
	var injected atomic.Bool
	var completionWrites atomic.Int32
	var nodeClient pb.DeviceProviderClient
	var owner *pb.Owner
	ready := make(chan struct{})
	acquireResult := make(chan *pb.Lease, 1)
	maps := &completionMapClient{ConfigMapInterface: base, before: func(ctx context.Context, next *core.ConfigMap) {
		var record struct {
			Busy       json.RawMessage
			LastNative *struct{ Intent struct{ Method string } }
		}
		if err := json.Unmarshal([]byte(next.Data["external-domain.v1"]), &record); err != nil {
			t.Error(err)
			return
		}
		if record.LastNative == nil || record.LastNative.Intent.Method != "DeleteVolume" || string(record.Busy) != "null" {
			return
		}
		completionWrites.Add(1)
		if !injected.CompareAndSwap(false, true) {
			return
		}
		<-ready
		// This interception is outside client-go's fake reactor lock:
		// the completion GET already happened, but its UPDATE has not.
		if effects.Load() != 1 {
			t.Errorf("completion preceded native effect")
		}
		acquired, err := nodeClient.Acquire(ctx, &pb.AcquireRequest{RequestId: "between-end-get-and-cas", Owner: owner})
		if err != nil {
			t.Errorf("interleaved Acquire failed: %v", err)
		}
		acquireResult <- acquired
	}}
	f.d.kubeClient = completionClient{Interface: f.kube, core: completionCore{CoreV1Interface: f.kube.CoreV1(), maps: maps}}
	native, controller := f.start(t)
	lease, err := controller.Reserve(t.Context(), &pb.ReserveRequest{RequestId: "reserve-origin", CompositeVolumeId: f.id,
		Pool: &pb.Pool{Provider: f.cfg.Identity.ProviderID, PoolId: f.cfg.Identity.PoolID}, CapacityBytes: 1 << 30})
	require.NoError(t, err)
	nodeClient = f.provider(t, f.nodeCert)
	owner = &pb.Owner{Allocation: lease.Allocation, NodeId: "vm1", OwnerEpoch: 1}
	close(ready)
	callCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	_, err = native.DeleteVolume(callCtx, &csi.DeleteVolumeRequest{VolumeId: other})
	require.NoError(t, err)
	require.Equal(t, int32(1), effects.Load())
	require.True(t, injected.Load())
	require.Equal(t, int32(2), completionWrites.Load(), "only the losing completion CAS is retried")
	acquired := <-acquireResult
	require.NotNil(t, acquired)
	cm, err := base.Get(t.Context(), "online", metav1.GetOptions{})
	require.NoError(t, err)
	var state struct {
		Busy       *json.RawMessage
		LastNative *json.RawMessage
	}
	require.NoError(t, json.Unmarshal([]byte(cm.Data["external-domain.v1"]), &state))
	require.Nil(t, state.Busy)
	require.NotNil(t, state.LastNative)
	require.Equal(t, "0", cm.Data["deadline"])
	require.Equal(t, "explicit-release", cm.Data["hold-policy.v1"])
	require.NotContains(t, cm.Data, "renewals.v1")
	require.Equal(t, "between-end-get-and-cas", cm.Data["acquireID"])
	require.Equal(t, "held", cm.Data["phase"])
	replay, err := nodeClient.Acquire(t.Context(), &pb.AcquireRequest{RequestId: "between-end-get-and-cas", Owner: owner})
	require.NoError(t, err)
	require.Equal(t, acquired, replay)
}
