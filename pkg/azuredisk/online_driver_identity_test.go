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

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOnlineRuntimeDriverMustMatchPinnedCanary(t *testing.T) {
	const origin = "disk.dmstack-canary.csi.infinitydb.io"
	for _, name := range []string{"disk.csi.azure.com", "localdisk.csi.acstor.io", strings.ToUpper(origin)} {
		t.Run(name, func(t *testing.T) {
			f := newOnlineDriverFixture(t, origin, "localdisk.dmstack-canary.csi.infinitydb.io")
			f.d.Name = name
			_, err := f.d.configureOnline(t.Context())
			require.ErrorContains(t, err, "exactly match")
			require.Nil(t, f.d.onlineDomain)
			called := false
			_, err = f.d.onlineNative(t.Context(), &csi.DeleteVolumeRequest{VolumeId: f.diskID},
				&grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/DeleteVolume"},
				func(context.Context, any) (any, error) { called = true; return &csi.DeleteVolumeResponse{}, nil })
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.False(t, called, "failed configuration cannot fall through to ordinary admission")
			_, err = f.d.DeleteVolume(t.Context(), &csi.DeleteVolumeRequest{VolumeId: f.diskID})
			require.Equal(t, codes.Unavailable, status.Code(err))
		})
	}
}

func TestOfficialDriverWithoutOnlineOptInIsUnchanged(t *testing.T) {
	driver, err := NewFakeDriver(gomock.NewController(t))
	require.NoError(t, err)
	d := &driver.(*fakeDriver).Driver
	require.Equal(t, "disk.csi.azure.com", d.Name)
	cfg, err := d.configureOnline(t.Context())
	require.NoError(t, err)
	require.Nil(t, cfg)
	require.Nil(t, d.onlineDomain)
	called := false
	response := &csi.ControllerExpandVolumeResponse{CapacityBytes: 4096}
	got, err := d.onlineNative(t.Context(), &csi.ControllerExpandVolumeRequest{VolumeId: "ordinary-volume"},
		&grpc.UnaryServerInfo{FullMethod: "/csi.v1.Controller/ControllerExpandVolume"},
		func(context.Context, any) (any, error) { called = true; return response, nil })
	require.NoError(t, err)
	require.True(t, called, "default addon requests must still reach the ordinary implementation")
	require.Same(t, response, got)
}
