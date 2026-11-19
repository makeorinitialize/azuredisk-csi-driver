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
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/diskclient/mock_diskclient"
	"sigs.k8s.io/cloud-provider-azure/pkg/azclient/mock_azclient"
)

type csiAuthority struct {
	devicelease.Session // unexpected owner operations must not be called by ordinary CSI
	t                   *testing.T
	busy                bool
	originID            string
}

func (a *csiAuthority) Begin(context.Context) (devicelease.Session, error) {
	require.False(a.t, a.busy)
	a.busy = true
	return a, nil
}
func (a *csiAuthority) End() { require.True(a.t, a.busy); a.busy = false }
func (a *csiAuthority) Lookup(_ context.Context, id string) (devicelease.Record, error) {
	require.True(a.t, a.busy)
	if strings.EqualFold(id, a.originID) {
		return devicelease.Record{Disposition: devicelease.Held}, nil
	}
	return devicelease.Record{Disposition: devicelease.Ordinary}, nil
}
func (a *csiAuthority) ProveOrdinary(context.Context, proto.Message, []string) error {
	require.True(a.t, a.busy)
	return nil
}

func TestOriginCSIGuards(t *testing.T) {
	ctx := t.Context()
	a := &csiAuthority{t: t, originID: testVolumeID}
	// No cloud, kube, mount or wireserver clients. Both controller and QAD
	// requests must reject the retained origin before reaching any of them.
	d := &Driver{origin: devicelease.NewOrigin(a), nodeDrivenAttachDetachEnabled: true}
	check := func(response any, err error) {
		t.Helper()
		require.Nil(t, response)
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.False(t, a.busy)
	}
	id := strings.ToUpper(testVolumeID)
	qad := map[string]string{consts.AttachModeField: consts.AttachModeNodeDriven}
	check(d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: id}))
	check(d.ControllerPublishVolume(ctx, &csi.ControllerPublishVolumeRequest{VolumeId: id, VolumeContext: qad}))
	check(d.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: id}))
	check(d.NodeStageVolume(ctx, &csi.NodeStageVolumeRequest{VolumeId: id, VolumeContext: qad}))
	check(d.NodeUnstageVolume(ctx, &csi.NodeUnstageVolumeRequest{VolumeId: id}))
	check(d.NodePublishVolume(ctx, &csi.NodePublishVolumeRequest{VolumeId: id}))
	check(d.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: id}))
	check(d.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id}))
	check(d.ControllerModifyVolume(ctx, &csi.ControllerModifyVolumeRequest{VolumeId: id}))
	check(d.ControllerExpandVolume(ctx, &csi.ControllerExpandVolumeRequest{VolumeId: id}))
	check(d.NodeExpandVolume(ctx, &csi.NodeExpandVolumeRequest{VolumeId: id}))
	check(d.NodeGetVolumeStats(ctx, &csi.NodeGetVolumeStatsRequest{VolumeId: id}))
	check(d.CreateSnapshot(ctx, &csi.CreateSnapshotRequest{SourceVolumeId: id}))

	fake, err := NewFakeDriver(gomock.NewController(t))
	require.NoError(t, err)
	fd := fake.(*fakeDriver)
	fd.origin = d.origin
	create := &csi.CreateVolumeRequest{Name: testVolumeName, VolumeCapabilities: stdVolumeCapabilities,
		Parameters: map[string]string{"resourceGroup": "rg", "subscriptionID": "subs"}}
	check(fd.CreateVolume(ctx, create))
	create.Name = "ordinary-target"
	create.VolumeContentSource = &csi.VolumeContentSource{Type: &csi.VolumeContentSource_Volume{Volume: &csi.VolumeContentSource_VolumeSource{VolumeId: id}}}
	check(fd.CreateVolume(ctx, create))
}

func TestOriginAdmissionHeldDuringDelete(t *testing.T) {
	ctrl := gomock.NewController(t)
	fake, err := NewFakeDriver(ctrl)
	require.NoError(t, err)
	d := fake.(*fakeDriver)
	a := &csiAuthority{t: t}
	d.origin = devicelease.NewOrigin(a)
	client := mock_diskclient.NewMockInterface(ctrl)
	d.getClientFactory().(*mock_azclient.MockClientFactory).EXPECT().GetDiskClientForSub("subs").Return(client, nil)
	client.EXPECT().Get(gomock.Any(), "rg", testVolumeName).Return(&armcompute.Disk{ID: &testVolumeID}, nil)
	client.EXPECT().Delete(gomock.Any(), "rg", testVolumeName).DoAndReturn(func(context.Context, string, string) error {
		require.True(t, a.busy, "check-then-delete window")
		return nil
	})
	_, err = d.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: testVolumeID})
	require.NoError(t, err)
	require.False(t, a.busy)
}
