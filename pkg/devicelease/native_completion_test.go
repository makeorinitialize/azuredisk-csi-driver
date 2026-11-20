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
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type completionMaps struct {
	coreclient.ConfigMapInterface
	update func(context.Context, *core.ConfigMap, metav1.UpdateOptions) (*core.ConfigMap, error)
}

func (c *completionMaps) Update(ctx context.Context, cm *core.ConfigMap, opts metav1.UpdateOptions) (*core.ConfigMap, error) {
	return c.update(ctx, cm, opts)
}

func TestNativeCompletionRefusesReplacement(t *testing.T) {
	for _, changed := range []string{"token", "method", "digest", "target"} {
		t.Run(changed, func(t *testing.T) {
			f := newOnlineFixture(t)
			ctx, _, err := f.d.BeginNative(t.Context(), "DeleteVolume", "native-input", resourceID, "", "")
			require.NoError(t, err)
			base := f.d.maps
			updates := 0
			f.d.maps = &completionMaps{ConfigMapInterface: base, update: func(ctx context.Context, next *core.ConfigMap, opts metav1.UpdateOptions) (*core.ConfigMap, error) {
				updates++
				current, err := base.Get(ctx, f.d.name, metav1.GetOptions{})
				require.NoError(t, err)
				var r onlineRecord
				require.NoError(t, json.Unmarshal([]byte(current.Data[onlineDomainKey]), &r))
				switch changed {
				case "token":
					r.Busy.ID = "successor"
				case "method":
					r.Busy.Method = "ControllerModifyVolume"
				case "digest":
					r.Busy.Digest = "changed-input"
				case "target":
					r.Busy.ResourceID = resourceID + "-successor"
				}
				raw, err := json.Marshal(r)
				require.NoError(t, err)
				current.Data[onlineDomainKey] = string(raw)
				_, err = base.Update(ctx, current, opts)
				require.NoError(t, err)
				return nil, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, next.Name, errors.New("changed during completion"))
			}}
			err = f.d.EndNative(ctx, true, "", []byte("actual-result"))
			require.Equal(t, codes.Aborted, status.Code(err))
			require.Equal(t, 1, updates, "a replaced operation must never get a completion write")
			_, r, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.NotNil(t, r.Busy)
			require.Nil(t, r.LastNative)
		})
	}
}

func TestNativeCompletionReconcilesOnlyExactHistory(t *testing.T) {
	for _, mode := range []string{"lost-reply", "unknown-before-write", "opaque-aborted"} {
		t.Run(mode, func(t *testing.T) {
			f := newOnlineFixture(t)
			ctx, _, err := f.d.BeginNative(t.Context(), "DeleteVolume", "native-input", resourceID, "", "")
			require.NoError(t, err)
			base := f.d.maps
			updates := 0
			f.d.maps = &completionMaps{ConfigMapInterface: base, update: func(ctx context.Context, cm *core.ConfigMap, opts metav1.UpdateOptions) (*core.ConfigMap, error) {
				updates++
				if mode == "lost-reply" {
					_, err := base.Update(ctx, cm, opts)
					require.NoError(t, err)
				}
				if mode == "opaque-aborted" {
					return nil, status.Error(codes.Aborted, "not an API resourceVersion conflict")
				}
				return nil, errUncertain
			}}
			outcome := []byte("native-response")
			err = f.d.EndNative(ctx, true, "result", outcome)
			require.Equal(t, 1, updates, "unknown outcomes do not permit blindly repeating completion CAS")
			if mode != "lost-reply" {
				require.Equal(t, codes.Unavailable, status.Code(err))
				_, r, err := f.d.read(t.Context())
				require.NoError(t, err)
				require.NotNil(t, r.Busy)
				require.Nil(t, r.LastNative)
				return
			}
			require.NoError(t, err)
			before, r, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.Nil(t, r.Busy)
			require.NotNil(t, r.LastNative)
			require.NoError(t, f.d.EndNative(ctx, true, "result", outcome))
			require.Equal(t, codes.Aborted, status.Code(f.d.EndNative(ctx, true, "result", []byte("changed-response"))))
			after, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, 1, updates, "history replays are read-only")
		})
	}
}
