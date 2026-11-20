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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOnlineLegacyDriverFieldsRemainReadable(t *testing.T) {
	f := newOnlineFixture(t)
	before, _, err := f.d.read(t.Context())
	require.NoError(t, err)
	require.NotContains(t, before.Data[onlineDomainKey], `"originDriver"`)
	require.NotContains(t, f.cv.GetAnnotations()[backingJournalKey], `"cacheDriver"`)
	cfg := f.d.identity
	cfg.Backing.OriginDriver = "disk.csi.azure.com"
	cfg.Backing.CacheDriver = "localdisk.csi.acstor.io"
	f.d, err = NewOnlineDomain(cfg, f.d.maps, f.d.name, f.d.uid, f.f.client, f.dyn)
	require.NoError(t, err)
	require.NoError(t, f.d.Check(t.Context()))
	after, _, err := f.d.read(t.Context())
	require.NoError(t, err)
	require.Equal(t, before, after, "default interpretation must not rewrite persisted legacy bytes")
	_, err = f.connect(t, ReservationController).Reserve(t.Context(), f.req)
	require.NoError(t, err)
	after, _, err = f.d.read(t.Context())
	require.NoError(t, err)
	require.NotContains(t, after.Data[onlineDomainKey], `"originDriver"`)
	require.NotContains(t, after.Data[onlineDomainKey], `"cacheDriver"`)
}

func TestOnlineCanaryRejectsOfficialAndAliasedSourceDrivers(t *testing.T) {
	const origin = "disk.dmstack-canary.csi.infinitydb.io"
	const cache = "localdisk.dmstack-canary.csi.infinitydb.io"
	for _, drift := range []string{"origin-class", "cache-class", "origin-pv", "cache-pv", "case-alias"} {
		t.Run(drift, func(t *testing.T) {
			f := newOnlineFixture(t, origin, cache)
			switch drift {
			case "origin-class", "cache-class", "case-alias":
				name, driver := "azure-block", "disk.csi.azure.com"
				if drift == "cache-class" {
					name, driver = "acs-block", "localdisk.csi.acstor.io"
				} else if drift == "case-alias" {
					driver = strings.ToUpper(origin)
				}
				sc, err := f.f.client.StorageV1().StorageClasses().Get(t.Context(), name, metav1.GetOptions{})
				require.NoError(t, err)
				sc.Provisioner = driver
				_, err = f.f.client.StorageV1().StorageClasses().Update(t.Context(), sc, metav1.UpdateOptions{})
				require.NoError(t, err)
			default:
				name, driver := f.j.Origin.PVName, "disk.csi.azure.com"
				if drift == "cache-pv" {
					name, driver = f.j.Cache.PVName, "localdisk.csi.acstor.io"
				}
				pv, err := f.f.client.CoreV1().PersistentVolumes().Get(t.Context(), name, metav1.GetOptions{})
				require.NoError(t, err)
				pv.Spec.CSI.Driver = driver
				pv.Annotations = map[string]string{"pv.kubernetes.io/provisioned-by": origin}
				_, err = f.f.client.CoreV1().PersistentVolumes().Update(t.Context(), pv, metav1.UpdateOptions{})
				require.NoError(t, err)
			}
			before, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			_, err = f.connect(t, ReservationController).Reserve(t.Context(), f.req)
			require.Equal(t, codes.FailedPrecondition, status.Code(err))
			after, _, err := f.d.read(t.Context())
			require.NoError(t, err)
			require.Equal(t, before, after, "foreign driver cannot consume enrollment or request history")
		})
	}
}

func TestOnlineDriverNamesCannotAlias(t *testing.T) {
	f := newOnlineFixture(t)
	for _, names := range [][2]string{
		{"same.example", "same.example"},
		{"Disk.Canary.Example", ""},
		{"*.canary.example", ""},
		{" disk.canary.example", ""},
		{"localdisk.csi.acstor.io", "cache.canary.example"},
		{"disk.canary.example", "disk.csi.azure.com"},
	} {
		cfg := f.d.identity
		cfg.Backing.OriginDriver, cfg.Backing.CacheDriver = names[0], names[1]
		_, err := InitialOnlineDomain(cfg)
		require.Equal(t, codes.InvalidArgument, status.Code(err), "%q", names)
	}
}
