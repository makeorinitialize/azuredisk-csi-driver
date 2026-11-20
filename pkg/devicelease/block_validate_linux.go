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
	"crypto/sha256"
	"errors"
	"os"
	"runtime"

	"golang.org/x/sys/unix"
)

// LinuxBlockCustody supplies borrowed, stable handles: the original O_RDWR OFD
// retained under exclusive provider ownership, and an independent duplicate of
// the authenticated facade's current FD. A transient O_EXCL probe is not the
// registered original: a lifetime exclusive claim conflicts with mkfs/DM and
// does not fence ordinary opens. Keep both descriptions and owner registration
// pinned through check. IdentifyOpened has the existing Session contract.
type LinuxBlockCustody interface {
	WithHandles(context.Context, BlockSelection, [sha256.Size]byte, func(registered BlockSelection, retained, current *os.File) error) error
	IdentifyOpened(context.Context, Binding, Owner, *os.File) (string, int64, error)
}

type LinuxBlockOpenAuthority struct{ Custody LinuxBlockCustody }

func (a LinuxBlockOpenAuthority) WithOpened(ctx context.Context, s BlockSelection, peer [sha256.Size]byte, check func(OpenedBlock) error) error {
	if a.Custody == nil {
		return errors.New("no node custody")
	}
	return a.Custody.WithHandles(ctx, s, peer, func(reg BlockSelection, retained, current *os.File) error {
		if reg != s || retained == nil || current == nil {
			return errors.New("node open registration unavailable")
		}
		if err := sameOpen(retained, current); err != nil {
			return err
		}
		var stat unix.Stat_t
		if err := unix.Fstat(int(current.Fd()), &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFBLK {
			return errors.New("not an open block device")
		}
		guid, size, err := a.Custody.IdentifyOpened(ctx, reg.Binding, reg.Owner, current)
		if err != nil {
			return err
		}
		if guid != reg.DiskUniqueID || size != reg.CapacityBytes {
			return errors.New("node disk identity or size mismatch")
		}
		return check(OpenedBlock{Selection: reg, Major: unix.Major(uint64(stat.Rdev)), Minor: unix.Minor(uint64(stat.Rdev))})
	})
}

func sameOpen(retained, current *os.File) error {
	defer runtime.KeepAlive(retained)
	defer runtime.KeepAlive(current)
	same, err := unix.FcntlInt(retained.Fd(), unix.F_DUPFD_QUERY, int(current.Fd()))
	if err != nil {
		return err
	}
	if same != 1 {
		return errors.New("open file description changed")
	}
	return nil
}
