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
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
	mockvm "sigs.k8s.io/cloud-provider-azure/pkg/azclient/virtualmachineclient/mock_virtualmachineclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/provider"
)

type holdMap struct {
	coreclient.ConfigMapInterface
	mu sync.Mutex
	cm *v1.ConfigMap
}

func TestPublishedBlockCrossResourceGroupDiskOperations(t *testing.T) {
	for _, operation := range []string{"delete", "modify", "expand", "publish", "unpublish"} {
		t.Run(operation, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			f, err := NewFakeDriver(ctrl)
			require.NoError(t, err)
			d := &f.(*fakeDriver).Driver
			selectedID := fmt.Sprintf("/subscriptions/%s/resourceGroups/rg-a/providers/Microsoft.Compute/disks/same-name", d.cloud.SubscriptionID)
			ordinaryID := strings.Replace(selectedID, "/rg-a/", "/rg-b/", 1)
			s := devicelease.BlockSelection{Provider: "provider", PoolID: "pool",
				Binding: devicelease.Binding{AllocationID: "allocation", CompositeID: "composite", ResourceID: selectedID, DiskUniqueID: "disk-guid", DeviceGeneration: 1, CapacityBytes: 1 << 30},
				Owner:   devicelease.Owner{NodeID: "vm1", Epoch: 5}, PV: "pv", PVUID: "pvuid", Namespace: "ns", PVC: "pvc", PVCUID: "pvcuid",
				VMID: fmt.Sprintf("/subscriptions/%s/resourceGroups/rg-a/providers/Microsoft.Compute/virtualMachines/vm1", d.cloud.SubscriptionID), VMGUID: "vm-guid"}
			data, err := devicelease.InitialBlockHold(s)
			require.NoError(t, err)
			data["phase"], data["acquireID"], data["deadline"] = "held", "acquire", strconv.FormatInt(time.Now().Add(time.Minute).Unix(), 10)
			store := &holdMap{cm: &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "hold", UID: "uid", ResourceVersion: "1"}, Data: data}}
			d.publishedBlock, err = devicelease.NewBlockHold(s, store, "hold", "uid", nil)
			require.NoError(t, err)
			pv := newTestPV(selectedID)
			pv.Name, pv.UID = s.PV, types.UID(s.PVUID)
			pv.Annotations = map[string]string{"pv.kubernetes.io/provisioned-by": d.Name}
			pv.Spec.VolumeMode = ptr.To(v1.PersistentVolumeBlock)
			pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
			pv.Spec.ClaimRef = &v1.ObjectReference{Namespace: s.Namespace, Name: s.PVC, UID: types.UID(s.PVCUID)}
			pv.Status.Phase = v1.VolumeBound
			pvc := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: s.Namespace, Name: s.PVC, UID: types.UID(s.PVCUID)},
				Spec: v1.PersistentVolumeClaimSpec{VolumeName: s.PV, VolumeMode: ptr.To(v1.PersistentVolumeBlock)}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
			d.kubeClient = fake.NewSimpleClientset(pv, pvc)
			invoke := func(id string) error {
				switch operation {
				case "delete":
					_, err := d.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: id})
					return err
				case "modify":
					_, err := d.ControllerModifyVolume(t.Context(), &csi.ControllerModifyVolumeRequest{VolumeId: id, MutableParameters: map[string]string{"skuName": "Premium_LRS"}})
					return err
				case "expand":
					_, err := d.ControllerExpandVolume(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: id, CapacityRange: &csi.CapacityRange{RequiredBytes: 2 << 30}})
					return err
				case "publish":
					_, err := d.ControllerPublishVolume(t.Context(), &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: s.NodeID, VolumeCapability: &csi.VolumeCapability{
						AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}})
					return err
				default:
					_, err := d.ControllerUnpublishVolume(t.Context(), &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: s.NodeID})
					return err
				}
			}
			// Every selected operation is denied before any disk-RP or VM
			// effect. Alternate ARM casing must not bypass selection.
			for _, id := range []string{selectedID, strings.ToUpper(selectedID)} {
				require.Equal(t, codes.FailedPrecondition, status.Code(invoke(id)))
			}
			before, err := store.Get(t.Context(), "hold", metav1.GetOptions{})
			require.NoError(t, err)
			if operation == "publish" || operation == "unpublish" {
				require.Equal(t, codes.FailedPrecondition, status.Code(invoke(ordinaryID)), "VM name matching remains ambiguous across RGs")
			} else {
				disk := &armcompute.Disk{ID: &ordinaryID, SKU: &armcompute.DiskSKU{Name: ptr.To(armcompute.DiskStorageAccountTypesStandardLRS)},
					Properties: &armcompute.DiskProperties{DiskSizeGB: ptr.To[int32](1), DiskState: ptr.To(armcompute.DiskStateUnattached)}}
				dc := mock_diskclient.NewMockInterface(ctrl)
				d.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(d.cloud.SubscriptionID).Return(dc, nil).AnyTimes()
				gets := 2
				if operation == "delete" {
					gets = 1
					dc.EXPECT().Delete(gomock.Any(), "rg-b", "same-name").Return(nil)
				} else {
					dc.EXPECT().Patch(gomock.Any(), "rg-b", "same-name", gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, patch armcompute.DiskUpdate) (*armcompute.Disk, error) {
						if operation == "expand" {
							require.Equal(t, int32(2), *patch.Properties.DiskSizeGB)
						} else {
							require.Equal(t, armcompute.DiskStorageAccountTypesPremiumLRS, *patch.SKU.Name)
						}
						return disk, nil
					})
				}
				dc.EXPECT().Get(gomock.Any(), "rg-b", "same-name").Return(disk, nil).Times(gets)
				require.NoError(t, invoke(ordinaryID), "full-ID disk operation must reach only RG B")
			}
			after, err := store.Get(t.Context(), "hold", metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, before, after, "unrelated disk must not alter selected hold")
		})
	}
}

func TestPublishedBlockMixedBatchForceDetach(t *testing.T) {
	for _, drainer := range []string{"disk1", "disk2"} {
		for _, forceFails := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/forceFails=%t", drainer, forceFails), func(t *testing.T) {
				ctrl := gomock.NewController(t)
				cloud := provider.GetTestCloud(ctrl)
				id := func(name string) string {
					return fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/%s", cloud.SubscriptionID, cloud.ResourceGroup, name)
				}
				selectedID, ordinaryID := id("disk1"), id("disk2")
				common := &controllerCommon{cloud: cloud, clientFactory: cloud.ComputeClientFactory, lockMap: newLockMap(),
					ForceDetachBackoff: true, DisableDiskLunCheck: true, DetachOperationMinTimeoutInSeconds: 30,
					noForceDiskID: strings.ToUpper(selectedID)}
				vm := setTestVirtualMachines(cloud, map[string]string{"vm1": "PowerState/Running"}, false)[0]
				for _, disk := range vm.Properties.StorageProfile.DataDisks {
					disk.ManagedDisk = &armcompute.ManagedDiskParameters{ID: ptr.To(id(*disk.Name))}
				}
				vc := cloud.ComputeClientFactory.GetVirtualMachineClient().(*mockvm.MockInterface)
				vc.EXPECT().Get(gomock.Any(), cloud.ResourceGroup, "vm1", gomock.Any()).Return(&vm, nil).AnyTimes()
				normalErr, forceErr := errors.New("normal batch detach failed"), errors.New("ordinary force detach failed")
				calls := 0
				vc.EXPECT().CreateOrUpdate(gomock.Any(), cloud.ResourceGroup, "vm1", gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, update armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
					calls++
					require.Len(t, update.Properties.StorageProfile.DataDisks, 3)
					for _, disk := range update.Properties.StorageProfile.DataDisks {
						switch *disk.ManagedDisk.ID {
						case selectedID:
							require.Nil(t, disk.DetachOption, "selected disk never enters force retry")
							if calls == 1 {
								require.True(t, ptr.Deref(disk.ToBeDetached, false), "selected disk participates in normal batch")
							}
						case ordinaryID:
							require.True(t, ptr.Deref(disk.ToBeDetached, false))
							if calls == 1 {
								require.Nil(t, disk.DetachOption)
							} else {
								require.Equal(t, armcompute.DiskDetachOptionTypesForceDetach, ptr.Deref(disk.DetachOption, ""))
							}
						default:
							require.Nil(t, disk.DetachOption)
							require.False(t, ptr.Deref(disk.ToBeDetached, false))
						}
					}
					if calls == 1 {
						return nil, normalErr
					}
					if forceFails {
						return nil, forceErr
					}
					return nil, nil
				}).Times(2)
				// Prequeue the same mixed batch, choosing either caller to drain
				// it deterministically instead of relying on goroutine timing.
				for _, name := range []string{"disk1", "disk2"} {
					_, err := common.batchDetachDiskRequest(name, strings.ToLower(id(name)), "vm1")
					require.NoError(t, err)
				}
				err := common.DetachDisk(t.Context(), drainer, id(drainer), types.NodeName("vm1"))
				if forceFails {
					require.ErrorIs(t, err, forceErr)
				} else if drainer == "disk1" {
					require.ErrorIs(t, err, normalErr, "ordinary retry cannot report selected disk success")
				} else {
					require.NoError(t, err, "selected exclusion must not suppress ordinary retry success")
				}
				require.Equal(t, 2, calls)
				remaining, err := common.getDetachDiskRequestNum("vm1")
				require.NoError(t, err)
				require.Zero(t, remaining)
			})
		}
	}
}

func (m *holdMap) Get(context.Context, string, metav1.GetOptions) (*v1.ConfigMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cm.DeepCopy(), nil
}
func (m *holdMap) Update(_ context.Context, c *v1.ConfigMap, _ metav1.UpdateOptions) (*v1.ConfigMap, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.ResourceVersion != m.cm.ResourceVersion {
		return nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, c.Name, errors.New("stale RV"))
	}
	n, _ := strconv.Atoi(c.ResourceVersion)
	m.cm = c.DeepCopy()
	m.cm.ResourceVersion = strconv.Itoa(n + 1)
	return m.cm.DeepCopy(), nil
}

type testCleanup struct {
	selection devicelease.BlockSelection
	unplugged bool
}

func (c *testCleanup) VerifyCleanup(_ context.Context, s devicelease.BlockSelection, id string) error {
	if s != c.selection || id != "receipt-1" || !c.unplugged {
		return errors.New("independent runtime teardown not confirmed")
	}
	return nil
}

func TestPublishedBlockControllerUnpublish(t *testing.T) {
	ctx := t.Context()
	ctrl := gomock.NewController(t)
	f, err := NewFakeDriver(ctrl)
	require.NoError(t, err)
	d := &f.(*fakeDriver).Driver
	vm := setTestVirtualMachines(d.cloud, map[string]string{"vm1": "PowerState/Running"}, false)[0]
	// setTestVirtualMachines uses a fixed subscription in its instance IDs.
	id := fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/disks/disk1", d.cloud.SubscriptionID, d.cloud.ResourceGroup)
	vm.ID = ptr.To(fmt.Sprintf("/subscriptions/%s/resourceGroups/%s/providers/Microsoft.Compute/virtualMachines/vm1", d.cloud.SubscriptionID, d.cloud.ResourceGroup))
	vm.Properties.VMID = ptr.To("vm-guid")
	vm.Properties.OSProfile = &armcompute.OSProfile{ComputerName: vm.Name}
	vm.Properties.StorageProfile.DataDisks[0].ManagedDisk = &armcompute.ManagedDiskParameters{ID: &id}
	s := devicelease.BlockSelection{Provider: "provider", PoolID: "pool", Binding: devicelease.Binding{AllocationID: "allocation", CompositeID: "composite", ResourceID: id, DiskUniqueID: "disk-guid", DeviceGeneration: 1, CapacityBytes: 1 << 30},
		Owner: devicelease.Owner{NodeID: "vm1", Epoch: 5}, PV: "pv", PVUID: "pvuid", Namespace: "ns", PVC: "pvc", PVCUID: "pvcuid", VMID: *vm.ID, VMGUID: *vm.Properties.VMID}
	data, err := devicelease.InitialBlockHold(s)
	require.NoError(t, err)
	store := &holdMap{cm: &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "hold", UID: "hold-uid", ResourceVersion: "1"}, Data: data}}
	cleanup := &testCleanup{selection: s}
	restart := func() {
		h, err := devicelease.NewBlockHold(s, store, "hold", "hold-uid", cleanup)
		require.NoError(t, err)
		d.publishedBlock = h
	}

	t.Run("stale acquire loses to teardown admission", func(t *testing.T) {
		ctx := t.Context()
		id := "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/disks/disk"
		s := devicelease.BlockSelection{Provider: "provider", PoolID: "pool", Binding: devicelease.Binding{AllocationID: "a", CompositeID: "c", ResourceID: id, DiskUniqueID: "guid", DeviceGeneration: 1, CapacityBytes: 4096},
			Owner: devicelease.Owner{NodeID: "node", Epoch: 7}, PV: "pv", PVUID: "pvuid", Namespace: "ns", PVC: "pvc", PVCUID: "pvcuid",
			VMID: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/vm", VMGUID: "instance"}
		data, err := devicelease.InitialBlockHold(s)
		require.NoError(t, err)
		store := &holdMap{cm: &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "hold", UID: "uid", ResourceVersion: "1"}, Data: data}}
		replica := func() *devicelease.BlockHold {
			h, err := devicelease.NewBlockHold(s, store, "hold", "uid", nil)
			require.NoError(t, err)
			return h
		}
		h := replica()
		done, err := h.BeginSetup(ctx, true)
		require.NoError(t, err)
		require.Error(t, replica().EnterTeardown(ctx))
		require.NoError(t, done())
		err = h.Resolve(ctx, s.Binding, s.Owner, func(context.Context, devicelease.BlockSelection) (*armcompute.Disk, devicelease.TargetVM, error) {
			// A second controller enters detach after the acquirer's GET and
			// before its CAS. A replacement process must see the same decision.
			require.NoError(t, replica().EnterTeardown(ctx))
			vm := devicelease.TargetVM{ID: s.VMID, InstanceGUID: s.VMGUID, ProvisioningState: "Succeeded", DataDisks: []*armcompute.DataDisk{{ManagedDisk: &armcompute.ManagedDiskParameters{ID: &id}, Lun: ptr.To[int32](1)}}}
			return &armcompute.Disk{ID: &id, ManagedBy: &s.VMID, Properties: &armcompute.DiskProperties{ProvisioningState: ptr.To("Succeeded"), UniqueID: &s.DiskUniqueID, DiskSizeBytes: &s.CapacityBytes,
				MaxShares: ptr.To[int32](1), DiskState: ptr.To(armcompute.DiskStateAttached)}}, vm, nil
		})
		require.Equal(t, codes.Aborted, status.Code(err))
		err = replica().Resolve(ctx, s.Binding, s.Owner, nil)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
	restart()
	d.diskController.noForceDiskID = id
	d.diskController.ForceDetachBackoff = true
	d.diskController.DisableDiskLunCheck = true
	pv := newTestPV(id)
	pv.Name = s.PV
	pv.UID = "pvuid"
	pv.Annotations = map[string]string{"pv.kubernetes.io/provisioned-by": d.Name}
	pv.Spec.VolumeMode = ptr.To(v1.PersistentVolumeBlock)
	pv.Spec.AccessModes = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
	pv.Spec.ClaimRef = &v1.ObjectReference{Namespace: "ns", Name: "pvc", UID: "pvcuid"}
	pv.Status.Phase = v1.VolumeBound
	pvc := &v1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "pvc", UID: "pvcuid"}, Spec: v1.PersistentVolumeClaimSpec{VolumeName: s.PV, VolumeMode: ptr.To(v1.PersistentVolumeBlock)}, Status: v1.PersistentVolumeClaimStatus{Phase: v1.ClaimBound}}
	d.kubeClient = fake.NewSimpleClientset(pv, pvc)
	disk := &armcompute.Disk{ID: &id, ManagedBy: vm.ID, Properties: &armcompute.DiskProperties{ProvisioningState: ptr.To("Succeeded"), UniqueID: ptr.To(s.DiskUniqueID), MaxShares: ptr.To[int32](1),
		DiskState: ptr.To(armcompute.DiskStateAttached), DiskSizeBytes: ptr.To(s.CapacityBytes)}}
	dc := mock_diskclient.NewMockInterface(ctrl)
	d.clientFactory.(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub(d.cloud.SubscriptionID).Return(dc, nil).AnyTimes()
	dc.EXPECT().Get(gomock.Any(), d.cloud.ResourceGroup, "disk1").Return(disk, nil).AnyTimes()
	vc := d.clientFactory.GetVirtualMachineClient().(*mockvm.MockInterface)
	vc.EXPECT().Get(gomock.Any(), d.cloud.ResourceGroup, "vm1", gomock.Any()).Return(&vm, nil).AnyTimes()
	publish := &csi.ControllerPublishVolumeRequest{VolumeId: id, NodeId: "vm1", VolumeCapability: &csi.VolumeCapability{AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}, AccessMode: &csi.VolumeCapability_AccessMode{Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}}}
	require.Error(t, d.ResolvePublishedBlock(ctx, s.Binding, s.Owner)) // not a successful ordinary publish yet
	_, err = d.ControllerPublishVolume(ctx, publish)
	require.NoError(t, err)
	disk.Properties.MaxShares = ptr.To[int32](2)
	require.Error(t, d.ResolvePublishedBlock(ctx, s.Binding, s.Owner))
	disk.Properties.MaxShares = ptr.To[int32](1)
	require.NoError(t, d.ResolvePublishedBlock(ctx, s.Binding, s.Owner))
	restart()
	require.NoError(t, d.ResolvePublishedBlock(ctx, s.Binding, s.Owner))
	unpublish := &csi.ControllerUnpublishVolumeRequest{VolumeId: id, NodeId: "vm1"}
	_, err = d.ControllerUnpublishVolume(ctx, unpublish)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Error(t, d.publishedBlock.ReleaseNode(ctx, s.Binding, s.Owner, "receipt-1"))
	_, err = d.ControllerUnpublishVolume(ctx, unpublish)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	cleanup.unplugged = true
	require.NoError(t, d.publishedBlock.ReleaseNode(ctx, s.Binding, s.Owner, "receipt-1"))
	restart()
	require.NoError(t, d.publishedBlock.ReleaseNode(ctx, s.Binding, s.Owner, "receipt-1"))
	// No cloud mutation expectation until after release. Both attempts enter
	// the actual normal detach helper; neither is allowed a force retry.
	vc.EXPECT().CreateOrUpdate(gomock.Any(), d.cloud.ResourceGroup, "vm1", gomock.Any()).Return(nil, errors.New("regular detach failed")).Times(1)
	_, err = d.ControllerUnpublishVolume(ctx, unpublish)
	require.Error(t, err)
	vc.EXPECT().CreateOrUpdate(gomock.Any(), d.cloud.ResourceGroup, "vm1", gomock.Any()).DoAndReturn(func(_ context.Context, _, _ string, update armcompute.VirtualMachine) (*armcompute.VirtualMachine, error) {
		require.True(t, *update.Properties.StorageProfile.DataDisks[0].ToBeDetached)
		require.Nil(t, update.Properties.StorageProfile.DataDisks[0].DetachOption)
		require.Error(t, d.ResolvePublishedBlock(ctx, s.Binding, s.Owner))
		return nil, nil
	})
	_, err = d.ControllerUnpublishVolume(ctx, unpublish)
	require.NoError(t, err)
}
