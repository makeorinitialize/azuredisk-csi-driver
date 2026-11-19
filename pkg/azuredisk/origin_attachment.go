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
	"fmt"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
)

type originAttachment struct{ d *Driver }

// WithOriginController opts a guarded controller into first-owner attachment.
// The ordinary CSI service and default reservation server remain unchanged.
func (d *Driver) WithOriginController(r *devicelease.Reservations) (*devicelease.Reservations, error) {
	if d.origin == nil || d.cloud == nil || d.diskController == nil || d.clientFactory == nil {
		return nil, fmt.Errorf("origin acquisition requires a guarded, initialized disk controller")
	}
	return r.WithControllerAttachment(originAttachment{d})
}

func (a originAttachment) Observe(ctx context.Context, node string) (devicelease.TargetVM, error) {
	d := a.d
	set, err := d.cloud.GetNodeVMSet(ctx, types.NodeName(node), azcache.CacheReadTypeForceRefresh)
	if err != nil {
		return devicelease.TargetVM{}, err
	}
	id, err := set.GetInstanceIDByNodeName(ctx, node)
	if err != nil {
		return devicelease.TargetVM{}, err
	}
	return a.readVM(ctx, node, id)
}

// The provider mapping selects a candidate only. GET that ARM resource and
// verify its own identity, incarnation, computer name and completed profile.
func (a originAttachment) readVM(ctx context.Context, node, id string) (devicelease.TargetVM, error) {
	d := a.d
	p := strings.Split(id, "/")
	if (len(p) != 9 && len(p) != 11) || p[0] != "" || !strings.EqualFold(p[1], "subscriptions") || !strings.EqualFold(p[2], d.cloud.SubscriptionID) ||
		!strings.EqualFold(p[3], "resourceGroups") || !strings.EqualFold(p[5], "providers") || !strings.EqualFold(p[6], "Microsoft.Compute") {
		return devicelease.TargetVM{}, fmt.Errorf("unsupported target VM resource ID")
	}
	var actual, guid, state, computer *string
	var storage *armcompute.StorageProfile
	switch {
	case len(p) == 9 && strings.EqualFold(p[7], "virtualMachines"):
		vm, err := d.clientFactory.GetVirtualMachineClient().Get(ctx, p[4], p[8], nil)
		if err != nil {
			return devicelease.TargetVM{}, err
		}
		if vm != nil && vm.Properties != nil && vm.Properties.OSProfile != nil {
			actual, guid, state, computer, storage = vm.ID, vm.Properties.VMID, vm.Properties.ProvisioningState, vm.Properties.OSProfile.ComputerName, vm.Properties.StorageProfile
		}
	case len(p) == 11 && strings.EqualFold(p[7], "virtualMachineScaleSets") && strings.EqualFold(p[9], "virtualMachines"):
		vm, err := d.clientFactory.GetVirtualMachineScaleSetVMClient().Get(ctx, p[4], p[8], p[10])
		if err != nil {
			return devicelease.TargetVM{}, err
		}
		if vm != nil && vm.Properties != nil && vm.Properties.OSProfile != nil {
			actual, guid, state, computer, storage = vm.ID, vm.Properties.VMID, vm.Properties.ProvisioningState, vm.Properties.OSProfile.ComputerName, vm.Properties.StorageProfile
		}
	}
	if !strings.EqualFold(ptr.Deref(actual, ""), id) || ptr.Deref(computer, "") != node || ptr.Deref(guid, "") == "" || storage == nil {
		return devicelease.TargetVM{}, fmt.Errorf("node/VM identity or storage profile not confirmed by Azure")
	}
	return devicelease.TargetVM{ID: *actual, InstanceGUID: *guid, ProvisioningState: ptr.Deref(state, ""), DataDisks: storage.DataDisks}, nil
}

func (a originAttachment) Dispatch(ctx context.Context, node string, disk *armcompute.Disk) error {
	id := *disk.ID
	name := id[strings.LastIndexByte(id, '/')+1:]
	// Use the same LUN allocator/VM update as ControllerPublishVolume, NOT
	// its recovery branch which can detach a dangling disk from another VM.
	n := types.NodeName(node)
	_, err := a.d.diskController.AttachDisk(ctx, name, id, n, armcompute.CachingTypesNone, disk, a.d.getOccupiedLunsFromNode(ctx, n, id))
	return err
}
