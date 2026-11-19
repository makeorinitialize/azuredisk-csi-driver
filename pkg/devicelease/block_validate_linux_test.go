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
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestPublishedBlockOpenDescription(t *testing.T) {
	// No physical device needed to distinguish a dup from a second open of
	// the same inode. The latter must not be accepted, even with identical stat.
	f, err := os.Open("block_validate_linux_test.go")
	require.NoError(t, err)
	defer f.Close()
	other, err := os.Open(f.Name())
	require.NoError(t, err)
	defer other.Close()
	dup, err := unix.Dup(int(f.Fd()))
	require.NoError(t, err)
	copy := os.NewFile(uintptr(dup), "custodian-dup")
	defer copy.Close()
	err = sameOpen(f, copy)
	if errors.Is(err, unix.EINVAL) {
		require.Error(t, sameOpen(f, other))
		t.Skip("kernel lacks F_DUPFD_QUERY; authority fails closed")
	}
	require.NoError(t, err)
	require.Error(t, sameOpen(f, other))
}
