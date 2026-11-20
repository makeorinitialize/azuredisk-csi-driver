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
	"k8s.io/utils/ptr"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease/api/v1"
)

func TestExternalExistingAttachmentRequiresExactStableOwner(t *testing.T) {
	for _, mode := range []string{"same-owner", "foreign-owner", "detaching", "shared"} {
		t.Run(mode, func(t *testing.T) {
			f := newOnlineFixture(t)
			f.attach()
			switch mode {
			case "foreign-owner":
				f.disk.ManagedBy = ptr.To("/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/other")
			case "detaching":
				f.vm.DataDisks[0].ToBeDetached = ptr.To(true)
			case "shared":
				f.disk.Properties.MaxShares = ptr.To[int32](2)
			}
			before, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			lease, err := f.connect(t, ReservationController).Reserve(t.Context(), f.req)
			if mode != "same-owner" {
				require.Error(t, err)
				after, _, err := f.d.read(t.Context())
				require.NoError(t, err)
				require.Equal(t, before, after, "no takeover or mutation of foreign/in-flight attachment")
				return
			}
			require.NoError(t, err)
			require.Equal(t, pb.LeaseState_LEASE_STATE_RESERVED, lease.State)
			cm, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.Equal(t, "published", cm.Data["phase"])
			_, err = f.connect(t, ReservationNode).Acquire(t.Context(), &pb.AcquireRequest{RequestId: "acquire",
				Owner: &pb.Owner{Allocation: lease.Allocation, NodeId: "vm1", OwnerEpoch: 1}})
			require.Equal(t, codes.OK, status.Code(err))
		})
	}
}
