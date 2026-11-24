// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package deviceuse

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse/usev1"
)

type memoryStore struct {
	mu       sync.Mutex
	data     []byte
	scope    Scope
	loseNext bool
}

func (m *memoryStore) Transact(_ context.Context, fn func(*Snapshot) (bool, error)) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, err := Decode(m.data, m.scope)
	if err != nil {
		return err
	}
	dirty, err := fn(s)
	if err != nil || !dirty {
		return err
	}
	if err := s.Validate(m.scope); err != nil {
		return err
	}
	m.data, err = json.Marshal(s)
	if m.loseNext {
		m.loseNext = false
		return errors.New("committed reply lost")
	}
	return err
}

type fakeResolver struct{ missing bool }

func (r *fakeResolver) CheckScope(_ context.Context, id *pb.UseIdentity) error {
	if r.missing || id.Source.PvUid != "pv-uid" {
		return status.Error(codes.FailedPrecondition, "PV missing or replaced")
	}
	return nil
}
func (r *fakeResolver) Resolve(ctx context.Context, id *pb.UseIdentity) (Resource, error) {
	if err := r.CheckScope(ctx, id); err != nil {
		return Resource{}, err
	}
	return Resource{Key: "actual-native-uuid", Target: "disk", Group: "group"}, nil
}

func fixture(t *testing.T) (*Domain, *memoryStore, *fakeResolver, *pb.UseIdentity) {
	t.Helper()
	scope := Scope{Driver: "disk.csi.azure.com", Namespace: "sources", NamespaceUID: "ns-uid", Node: "node", NodeUID: "node-uid"}
	s, err := Initial(scope)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(s)
	store := &memoryStore{data: b, scope: scope}
	resolver := &fakeResolver{}
	d := &Domain{Scope: scope, Store: store, Resolver: resolver}
	id := &pb.UseIdentity{UseId: "use-1", CompositeId: "compositedisk-" + strings.Repeat("a", 52) + ".cr-uid", NodeId: "node",
		Source: &pb.SourceVolume{Driver: scope.Driver, VolumeHandle: "disk", PvName: "pv", PvUid: "pv-uid"}}
	return d, store, resolver, id
}

func requireCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("code=%v want=%v err=%v", status.Code(err), code, err)
	}
}

func TestUsePersistsBlocksTeardownAndReleasesWithoutMetadata(t *testing.T) {
	d, store, resolver, id := fixture(t)
	ctx := t.Context()
	got, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_ACTIVE {
		t.Fatal(got)
	}
	restarted := &Domain{Scope: d.Scope, Store: store, Resolver: resolver}
	for _, method := range []string{"DeleteVolume", "ControllerUnpublishVolume", "NodeUnpublishVolume", "NodeUnstageVolume", "GC", "LVM"} {
		_, _, err = restarted.BeginNative(ctx, method, "disk", "")
		requireCode(t, err, codes.FailedPrecondition)
	}
	requireCode(t, restarted.CheckPrepare(ctx, "disk"), codes.OK)
	resolver.missing = true
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.FailedPrecondition)
	got, err = restarted.GetUse(ctx, &pb.GetUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	if got.State != pb.UseState_USE_STATE_ACTIVE {
		t.Fatal("metadata absence cleared hold")
	}
	_, err = restarted.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, finish, err := restarted.BeginNative(ctx, "DeleteVolume", "disk", "")
	requireCode(t, err, codes.OK)
	requireCode(t, finish(true), codes.OK)
}

func TestUseTerminalEarlyReleaseAndSuccessorIsolation(t *testing.T) {
	d, _, _, id := fixture(t)
	ctx := t.Context()
	_, err := d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.FailedPrecondition)
	next := proto.CloneOf(id)
	next.UseId = "use-2"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	requireCode(t, err, codes.OK)
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, _, err = d.BeginNative(ctx, "NodeUnstageVolume", "disk", "")
	requireCode(t, err, codes.FailedPrecondition)
	conflict := proto.CloneOf(next)
	conflict.Source.PvName = "alias"
	_, err = d.GetUse(ctx, &pb.GetUseRequest{Identity: conflict})
	requireCode(t, err, codes.AlreadyExists)
}

func TestUseNativeAdmissionWinsAgainstAcquireAndUnknownOutcome(t *testing.T) {
	d, store, resolver, id := fixture(t)
	ctx := t.Context()
	_, finish, err := d.BeginNative(ctx, "NodeUnstageVolume", "disk", "")
	requireCode(t, err, codes.OK)
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	requireCode(t, finish(false), codes.OK)
	restarted := &Domain{Scope: d.Scope, Store: store, Resolver: resolver}
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	store.loseNext = true
	requireCode(t, finish(true), codes.OK)
	_, err = restarted.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	_, _, err = restarted.BeginNative(ctx, "NodeUnstageVolume", "disk", "")
	requireCode(t, err, codes.FailedPrecondition)
}

func TestUseLostRepliesAliasesAndUIDReplacement(t *testing.T) {
	d, store, _, id := fixture(t)
	ctx := t.Context()
	store.loseNext = true
	if _, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id}); err == nil {
		t.Fatal("lost reply treated as confirmed")
	}
	_, err := d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	alias := proto.CloneOf(id)
	alias.UseId, alias.Source.PvName, alias.Source.VolumeHandle = "alias-use", "alias", "alias-handle"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: alias})
	requireCode(t, err, codes.AlreadyExists)
	store.loseNext = true
	if _, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id}); err == nil {
		t.Fatal("lost release reply treated as confirmed")
	}
	_, err = d.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
	alias.Source.PvUid = "replacement"
	_, err = d.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: alias})
	requireCode(t, err, codes.FailedPrecondition)
	missing := proto.CloneOf(id)
	missing.UseId = "never-observed"
	_, err = d.GetUse(ctx, &pb.GetUseRequest{Identity: missing})
	requireCode(t, err, codes.NotFound)
}

func TestUseConcurrentAcquisitionHasOneActualResourceOwner(t *testing.T) {
	d, _, _, id := fixture(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, key := range []string{"one", "two"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			q := proto.CloneOf(id)
			q.UseId = key
			_, err := d.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: q})
			results <- err
		}(key)
	}
	wg.Wait()
	close(results)
	success := 0
	for err := range results {
		if err == nil {
			success++
		} else {
			requireCode(t, err, codes.AlreadyExists)
		}
	}
	if success != 1 {
		t.Fatalf("actual resource has %d owners", success)
	}
}
