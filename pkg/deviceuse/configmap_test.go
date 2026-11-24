// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package deviceuse

import (
	"errors"
	"strconv"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse/usev1"
)

type casJournal struct {
	mu       sync.Mutex
	current  *core.ConfigMap
	loseNext bool
}

func casFixture(t *testing.T, scope Scope) (ConfigMapStore, *casJournal) {
	t.Helper()
	data, err := InitialData(scope)
	if err != nil {
		t.Fatal(err)
	}
	j := &casJournal{current: &core.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: "uses", Namespace: "journal", UID: "journal-uid", ResourceVersion: "1"}, Data: data}}
	kube := fake.NewSimpleClientset()
	kube.PrependReactor("get", "configmaps", func(k8stesting.Action) (bool, runtime.Object, error) {
		j.mu.Lock()
		defer j.mu.Unlock()
		return true, j.current.DeepCopy(), nil
	})
	kube.PrependReactor("update", "configmaps", func(action k8stesting.Action) (bool, runtime.Object, error) {
		j.mu.Lock()
		defer j.mu.Unlock()
		next := action.(k8stesting.UpdateAction).GetObject().(*core.ConfigMap)
		if next.ResourceVersion != j.current.ResourceVersion {
			return true, nil, apierrors.NewConflict(core.Resource("configmaps"), "uses", errors.New("lost CAS"))
		}
		version, _ := strconv.Atoi(j.current.ResourceVersion)
		j.current = next.DeepCopy()
		j.current.ResourceVersion = strconv.Itoa(version + 1)
		if j.loseNext {
			j.loseNext = false
			return true, nil, errors.New("committed reply lost")
		}
		return true, j.current.DeepCopy(), nil
	})
	return ConfigMapStore{Scope: scope, Client: kube.CoreV1().ConfigMaps("journal"), Name: "uses", UID: "journal-uid"}, j
}

func TestConfigMapCASSerializesReplicasAndRetainsLostNativeOutcome(t *testing.T) {
	d, _, resolver, id := fixture(t)
	store, journal := casFixture(t, d.Scope)
	d.Store = store
	other := &Domain{Scope: d.Scope, Store: store, Resolver: resolver}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i, instance := range []*Domain{d, other} {
		wg.Add(1)
		go func(i int, instance *Domain) {
			defer wg.Done()
			q := proto.CloneOf(id)
			q.UseId = []string{"one", "two"}[i]
			_, err := instance.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: q})
			results <- err
		}(i, instance)
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
		t.Fatal("CAS permitted competing actual resource owners", success)
	}
	// A missing source API object cannot erase either the durable use or admission.
	resolver.missing = true
	_, _, err := other.BeginNative(t.Context(), "DeleteVolume", "disk", "")
	requireCode(t, err, codes.FailedPrecondition)
	resolver.missing = false
	for _, key := range []string{"one", "two"} {
		q := proto.CloneOf(id)
		q.UseId = key
		_, err = d.ReleaseUse(t.Context(), &pb.ReleaseUseRequest{Identity: q})
		requireCode(t, err, codes.OK)
	}
	_, finish, err := d.BeginNative(t.Context(), "ControllerUnpublishVolume", "disk", "")
	requireCode(t, err, codes.OK)
	requireCode(t, finish(false), codes.OK)
	restart := &Domain{Scope: d.Scope, Store: store, Resolver: resolver}
	_, err = restart.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	journal.loseNext = true
	requireCode(t, finish(true), codes.OK)
	_, nextFinish, err := restart.BeginNative(t.Context(), "NodeUnstageVolume", "disk", "")
	requireCode(t, err, codes.OK)
	requireCode(t, finish(true), codes.OK)
	_, err = d.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.Unavailable)
	requireCode(t, nextFinish(true), codes.OK)
	_, err = d.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
	requireCode(t, err, codes.OK)
}

func TestConfigMapRejectsReplacementAndLegacyWithoutWriting(t *testing.T) {
	d, _, _, _ := fixture(t)
	store, journal := casFixture(t, d.Scope)
	for _, mutate := range []func(*core.ConfigMap){
		func(cm *core.ConfigMap) { cm.UID = "replacement" },
		func(cm *core.ConfigMap) { cm.Data = map[string]string{"external-domain.v1": "{}"} },
		func(cm *core.ConfigMap) { cm.Data[JournalKey] = `{"Version":2}` },
	} {
		data, err := InitialData(d.Scope)
		if err != nil {
			t.Fatal(err)
		}
		journal.current.UID, journal.current.Data = "journal-uid", data
		mutate(journal.current)
		before := journal.current.DeepCopy()
		called := false
		err = store.Transact(t.Context(), func(*Snapshot) (bool, error) { called = true; return true, nil })
		if err == nil || called || journal.current.ResourceVersion != before.ResourceVersion {
			t.Fatal("invalid journal admitted or rewritten")
		}
	}
}
