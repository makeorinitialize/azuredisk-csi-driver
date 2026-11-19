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
	"encoding/hex"
	"encoding/json"
	"math"
	"unicode/utf8"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	kjson "sigs.k8s.io/json"
)

const (
	journalKey = "origin-journal.json"
	gib        = 1 << 30
)

// RawPool enrolls a single fixed-size origin slot, never reused. ResourceID
// is the provider's preselected ARM target, not a caller-selected disk name.
// InventoryGUID pins a provider-owned, preprovisioned empty disk. If absent,
// only a new empty disk may be created in an exclusively controlled slot.
type RawPool struct {
	Provider, PoolID, ResourceID, Location, InventoryGUID string
	Bytes                                                 int64
}

func (p RawPool) valid() bool {
	_, err := armKey(p.ResourceID)
	return err == nil && validID(p.Provider) && validID(p.PoolID) && validID(p.Location) &&
		utf8.ValidString(p.InventoryGUID) && p.Bytes > 0 && p.Bytes%gib == 0 && p.Bytes/gib <= math.MaxInt32
}

func validID(s string) bool { return s != "" && utf8.ValidString(s) }

type reserveIntent struct {
	Method, Pool, Composite string
	Bytes                   uint64
}

type ledger struct {
	Version  int
	Pool     RawPool
	Requests map[string]reserveIntent
	First    string // original request key; the allocation remains charged
	Phase    string // free, submitted, reserved; missing is never free
	GUID     string
}

// InitialJournal is for explicit one-time bootstrap into a NEW provider
// domain. The serving path never creates, repairs or resets a journal.
func InitialJournal(pool RawPool) (map[string]string, error) {
	if !pool.valid() {
		return nil, status.Error(codes.InvalidArgument, "invalid origin pool")
	}
	b, err := json.Marshal(ledger{Version: 1, Pool: pool, Phase: "free", Requests: map[string]reserveIntent{}})
	if err != nil {
		return nil, err
	}
	return map[string]string{journalKey: string(b)}, nil
}

type journal struct {
	maps coreclient.ConfigMapInterface
	name string
	uid  types.UID
	pool RawPool
}

func (j journal) allocationID() string {
	// One slot, one allocation ever, one pinned API object incarnation.
	h := sha256.Sum256([]byte("azuredisk-origin/v1:" + string(j.uid)))
	return "origin-" + hex.EncodeToString(h[:])
}

func (j journal) read(ctx context.Context) (*corev1.ConfigMap, *ledger, error) {
	cm, err := j.maps.Get(ctx, j.name, metav1.GetOptions{})
	if err != nil {
		return nil, nil, unavailable(err)
	}
	if cm == nil || cm.UID != j.uid || cm.ResourceVersion == "" || cm.DeletionTimestamp != nil {
		return nil, nil, status.Error(codes.Unavailable, "origin journal incarnation unavailable")
	}
	var l ledger
	raw := cm.Data[journalKey]
	strict, err := kjson.UnmarshalStrict([]byte(raw), &l)
	if !utf8.ValidString(raw) || err != nil || len(strict) != 0 || !l.valid(j.pool) {
		return nil, nil, status.Error(codes.Unavailable, "unknown or inconsistent origin journal")
	}
	return cm, &l, nil
}

func (l *ledger) valid(pool RawPool) bool {
	if l.Version != 1 || l.Pool != pool || l.Requests == nil {
		return false
	}
	for k, r := range l.Requests {
		if !validID(k) || r.Method != "Reserve" || !validID(r.Pool) || !validID(r.Composite) || r.Bytes == 0 {
			return false
		}
	}
	if l.Phase == "free" {
		return l.First == "" && l.GUID == ""
	}
	first, ok := l.Requests[l.First]
	if !ok || first.Pool != pool.PoolID || first.Bytes > uint64(pool.Bytes) {
		return false
	}
	switch l.Phase {
	case "submitted":
		return l.GUID == "" && pool.InventoryGUID == ""
	case "reserved":
		return validID(l.GUID) && (pool.InventoryGUID == "" || pool.InventoryGUID == l.GUID)
	default:
		return false
	}
}

// save issues one resourceVersion CAS against the pinned UID. Only a
// definitive conflict may retry automatically. A lost Update response is
// not evidence that the mutation failed.
func (j journal) save(ctx context.Context, cm *corev1.ConfigMap, l *ledger) (bool, error) {
	b, err := json.Marshal(l)
	if err != nil {
		return false, unavailable(err)
	}
	if len(b) > 900*1024 {
		return false, status.Error(codes.Unavailable, "origin request ledger full; cannot evict history")
	}
	update := cm.DeepCopy()
	update.Data[journalKey] = string(b)
	saved, err := j.maps.Update(ctx, update, metav1.UpdateOptions{})
	if apierrors.IsConflict(err) {
		return false, nil
	}
	if err != nil {
		return false, unavailable(err)
	}
	if saved == nil || saved.UID != j.uid || saved.ResourceVersion == "" || saved.DeletionTimestamp != nil || saved.Data[journalKey] != string(b) {
		return false, status.Error(codes.Unavailable, "unconfirmed origin journal update")
	}
	return true, nil
}
