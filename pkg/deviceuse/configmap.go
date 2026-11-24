// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package deviceuse

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

const JournalKey = "device-use.v1"

type ConfigMapStore struct {
	Client coreclient.ConfigMapInterface
	Name   string
	UID    types.UID
	Scope  Scope
}

func InitialData(scope Scope) (map[string]string, error) {
	s, err := Initial(scope)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(s)
	return map[string]string{JournalKey: string(raw)}, err
}

func Decode(raw []byte, scope Scope) (*Snapshot, error) {
	if len(raw) > journalLimit {
		return nil, status.Error(codes.Unavailable, "use journal exceeds limit")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	var s Snapshot
	if err := d.Decode(&s); err != nil {
		return nil, status.Error(codes.Unavailable, "invalid use journal")
	}
	if !errors.Is(d.Decode(new(any)), io.EOF) {
		return nil, status.Error(codes.Unavailable, "trailing use journal data")
	}
	if err := s.Validate(scope); err != nil {
		return nil, err
	}
	canonical, err := json.Marshal(&s)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) {
		return nil, status.Error(codes.Unavailable, "noncanonical or duplicate use journal fields")
	}
	return &s, nil
}

func (s ConfigMapStore) Transact(ctx context.Context, fn func(*Snapshot) (bool, error)) error {
	for attempt := 0; attempt < 16; attempt++ {
		cm, err := s.Client.Get(ctx, s.Name, metav1.GetOptions{})
		if err != nil || cm == nil || s.UID == "" || cm.UID != s.UID || cm.ResourceVersion == "" ||
			cm.DeletionTimestamp != nil || len(cm.Data) != 1 {
			return status.Error(codes.Unavailable, "pinned use journal unavailable")
		}
		record, err := Decode([]byte(cm.Data[JournalKey]), s.Scope)
		if err != nil {
			return err
		}
		changed, err := fn(record)
		if err != nil || !changed {
			return err
		}
		if err := record.Validate(s.Scope); err != nil {
			return err
		}
		data, err := json.Marshal(record)
		if err != nil || len(data) > journalLimit {
			return status.Error(codes.ResourceExhausted, "use journal capacity reached")
		}
		next := cm.DeepCopy()
		next.Data[JournalKey] = string(data)
		saved, err := s.Client.Update(ctx, next, metav1.UpdateOptions{})
		if apierrors.IsConflict(err) {
			continue // Only a confirmed lost CAS permits another write attempt.
		}
		if err != nil {
			return status.Error(codes.Unavailable, "use journal write outcome unknown")
		}
		if saved == nil || saved.UID != s.UID || saved.ResourceVersion == "" || saved.ResourceVersion == cm.ResourceVersion ||
			saved.DeletionTimestamp != nil || !maps.Equal(saved.Data, next.Data) {
			return status.Error(codes.Unavailable, "use journal commit unconfirmed")
		}
		return nil
	}
	return status.Error(codes.Aborted, "use journal CAS contention")
}
