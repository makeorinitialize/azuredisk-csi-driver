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
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestOnlineNativeGateExcludesOverlappingOperationsAcrossReplicas(t *testing.T) {
	f := newOnlineFixture(t)
	client := f.connect(t, ReservationController)
	lease, err := client.Reserve(t.Context(), f.req)
	require.NoError(t, err)
	admitter := f.d
	ctx, hold, err := admitter.BeginNative(t.Context(), "ControllerPublishVolume", "native-open-operation", resourceID, "", "")
	require.NoError(t, err)
	require.NotNil(t, hold)
	finish, err := hold.BeginSetup(ctx, true)
	require.NoError(t, err)
	before, _, err := admitter.read(t.Context())
	require.NoError(t, err)

	// This exercises the real admission implementation, not a local mutex or
	// an O_EXCL stand-in. Hardware/OFD behavior is qualified separately in a
	// disposable guest; no block device is opened by this test.
	f.restart(t)
	for _, method := range []string{"ControllerPublishVolume", "NodeStageVolume", "NodePublishVolume",
		"ControllerUnpublishVolume", "NodeUnstageVolume", "NodeUnpublishVolume", "DeleteVolume"} {
		_, _, err := f.d.BeginNative(t.Context(), method, "overlap-"+method, resourceID, "", "")
		require.Equal(t, codes.Unavailable, status.Code(err), method)
	}
	_, err = client.Reserve(t.Context(), f.req)
	require.Equal(t, codes.Unavailable, status.Code(err))
	_, err = client.GetLease(t.Context(), lease.Allocation.Key)
	require.Equal(t, codes.Unavailable, status.Code(err))
	after, _, err := admitter.read(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after, "rejected overlaps must not clear or rewrite admission")

	require.NoError(t, finish())
	require.NoError(t, admitter.EndNative(ctx, true, "", nil))
	ctx2, _, err := f.d.BeginNative(t.Context(), "NodeStageVolume", "next-operation", resourceID, "", "")
	require.NoError(t, err)
	before, _, err = f.d.read(t.Context())
	require.NoError(t, err)
	require.NoError(t, admitter.EndNative(ctx, true, "", nil), "exact completed history acknowledges only the old operation")
	after, _, err = f.d.read(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after, "historical completion must not release a successor's gate")
	require.NoError(t, f.d.EndNative(ctx2, false, "", nil))
	f.restart(t)
	_, _, err = f.d.BeginNative(t.Context(), "DeleteVolume", "after-uncertain-open", resourceID, "", "")
	require.Equal(t, codes.Unavailable, status.Code(err), "uncertain work stays retained after restart")
}
