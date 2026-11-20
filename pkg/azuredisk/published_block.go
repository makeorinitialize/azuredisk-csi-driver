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
	"crypto/tls"
	"crypto/x509"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
)

type blockSetupRequest interface {
	proto.Message
	GetVolumeId() string
	GetVolumeCapability() *csi.VolumeCapability
	GetVolumeContext() map[string]string
}

func runBlockSetup[Q blockSetupRequest, R any](ctx context.Context, d *Driver, q Q, node string, publish bool, run func(context.Context, Q) (*R, error)) (*R, error) {
	if d.onlineDomain != nil && !d.onlineDomain.NativeAdmitted(ctx) {
		return nil, status.Error(codes.FailedPrecondition, "online native admission required")
	}
	h := d.publishedHold(ctx)
	if !h.Matches(q.GetVolumeId()) {
		end, err := d.guardDisk(ctx, q, q.GetVolumeId())
		if err != nil {
			return nil, err
		}
		defer end()
		return run(ctx, q)
	}
	mode, err := getAttachMode(q.GetVolumeContext())
	shares, sharesErr := azureutils.GetMaxShares(q.GetVolumeContext())
	if d.origin != nil || node != h.Selection().NodeID || q.GetVolumeCapability().GetBlock() == nil || q.GetVolumeCapability().GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER ||
		err != nil || mode != azureconstants.AttachModeControllerDriven || sharesErr != nil || shares != 1 {
		return nil, status.Error(codes.FailedPrecondition, "selected PV requires controller-driven exclusive block publication to enrolled node")
	}
	if err = d.verifyBlockPV(ctx, h.Selection()); err != nil {
		return nil, err
	}
	finish, err := h.BeginSetup(ctx, publish)
	if err != nil {
		return nil, err
	}
	resp, err := run(ctx, q)
	if err != nil {
		return nil, err
	} // ambiguous physical work: retain setup marker
	if err = finish(); err != nil {
		return nil, err
	}
	return resp, nil
}

func (d *Driver) verifyBlockPV(ctx context.Context, s devicelease.BlockSelection) error {
	if d.kubeClient == nil {
		return status.Error(codes.Unavailable, "live PV and PVC clients required")
	}
	pv, err := d.kubeClient.CoreV1().PersistentVolumes().Get(ctx, s.PV, metav1.GetOptions{})
	if err != nil {
		return status.Error(codes.Unavailable, "enrolled PV unavailable")
	}
	src, ref := pv.Spec.CSI, pv.Spec.ClaimRef
	if string(pv.UID) != s.PVUID || pv.DeletionTimestamp != nil || pv.Status.Phase != v1.VolumeBound ||
		d.onlineDomain == nil && pv.Annotations["pv.kubernetes.io/provisioned-by"] != d.Name ||
		src == nil || src.Driver != d.Name || !strings.EqualFold(src.VolumeHandle, s.ResourceID) || src.ReadOnly || d.hasQADInfo(pv) ||
		ptr.Deref(pv.Spec.VolumeMode, v1.PersistentVolumeFilesystem) != v1.PersistentVolumeBlock || len(pv.Spec.AccessModes) != 1 || pv.Spec.AccessModes[0] != v1.ReadWriteOnce ||
		ref == nil || ref.Name != s.PVC || ref.Namespace != s.Namespace || string(ref.UID) != s.PVCUID {
		return status.Error(codes.FailedPrecondition, "enrolled CSI block PV mismatch")
	}
	mode, err := getAttachMode(src.VolumeAttributes)
	shares, shareErr := azureutils.GetMaxShares(src.VolumeAttributes)
	if err != nil || mode != azureconstants.AttachModeControllerDriven || shareErr != nil || shares != 1 {
		return status.Error(codes.FailedPrecondition, "shared or node-driven enrolled PV")
	}
	pvc, err := d.kubeClient.CoreV1().PersistentVolumeClaims(s.Namespace).Get(ctx, s.PVC, metav1.GetOptions{})
	if err != nil {
		return status.Error(codes.Unavailable, "enrolled PVC unavailable")
	}
	if string(pvc.UID) != s.PVCUID || pvc.DeletionTimestamp != nil || pvc.Status.Phase != v1.ClaimBound || pvc.Spec.VolumeName != pv.Name || ptr.Deref(pvc.Spec.VolumeMode, v1.PersistentVolumeFilesystem) != v1.PersistentVolumeBlock {
		return status.Error(codes.FailedPrecondition, "enrolled block PVC mismatch")
	}
	return nil
}

// ResolvePublishedBlock does not publish, dispatch an attach, open a device,
// or mint a cleanup receipt. Caller authentication is required by its host.
func (d *Driver) ResolvePublishedBlock(ctx context.Context, b devicelease.Binding, o devicelease.Owner) error {
	if d.publishedBlock == nil || d.origin != nil {
		return status.Error(codes.FailedPrecondition, "ordinary block hold disabled")
	}
	return d.publishedBlock.Resolve(ctx, b, o, d.observeBlock)
}

// PublishedBlockServer registers the hold on the existing DeviceProvider v1
// mTLS transport. CSI startup never constructs or starts this server.
func (d *Driver) PublishedBlockServer(cert tls.Certificate, roots *x509.CertPool, grants []devicelease.ReservationGrant) (*grpc.Server, error) {
	if d.origin != nil {
		return nil, status.Error(codes.FailedPrecondition, "incompatible origin modes")
	}
	return devicelease.NewPublishedBlockServer(d.publishedBlock, d.observeBlock, cert, roots, grants)
}

func (d *Driver) observeBlock(ctx context.Context, s devicelease.BlockSelection) (*armcompute.Disk, devicelease.TargetVM, error) {
	if err := d.verifyBlockPV(ctx, s); err != nil {
		return nil, devicelease.TargetVM{}, err
	}

	if d.diskController == nil || d.cloud == nil || d.clientFactory == nil {
		return nil, devicelease.TargetVM{}, status.Error(codes.Unavailable, "controller not initialized")
	}
	vm, err := (originAttachment{d}).Observe(ctx, s.NodeID)
	if err != nil {
		return nil, vm, status.Error(codes.Unavailable, "live target VM unavailable")
	}
	disk, err := d.diskController.GetDiskByURI(ctx, s.ResourceID)
	if err != nil {
		return nil, vm, status.Error(codes.Unavailable, "enrolled disk unavailable")
	}
	return disk, vm, nil
}

// Existing ownership is not contingent on source API objects surviving. The
// retained VM/disk identities still require fresh independent Azure evidence;
// missing resources or changed incarnations are uncertainty, never release.
func (d *Driver) observeRetainedBlock(ctx context.Context, s devicelease.BlockSelection) (*armcompute.Disk, devicelease.TargetVM, error) {
	if d.diskController == nil || d.cloud == nil || d.clientFactory == nil {
		return nil, devicelease.TargetVM{}, status.Error(codes.Unavailable, "controller not initialized")
	}
	vm, err := (originAttachment{d}).readVM(ctx, s.NodeID, s.VMID)
	if err != nil {
		return nil, vm, status.Error(codes.Unavailable, "retained VM unavailable")
	}
	disk, err := d.diskController.GetDiskByURI(ctx, s.ResourceID)
	if err != nil {
		return nil, vm, status.Error(codes.Unavailable, "retained disk unavailable")
	}
	return disk, vm, nil
}
