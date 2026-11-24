// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package azuredisk

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse"
	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse/usev1"
)

type useTestStore struct {
	mu    sync.Mutex
	data  []byte
	scope deviceuse.Scope
}

func (s *useTestStore) Transact(_ context.Context, fn func(*deviceuse.Snapshot) (bool, error)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := deviceuse.Decode(s.data, s.scope)
	if err != nil {
		return err
	}
	dirty, err := fn(record)
	if err != nil || !dirty {
		return err
	}
	s.data, err = json.Marshal(record)
	return err
}

type useTestSource struct{ target string }

func (useTestSource) CheckScope(context.Context, *pb.UseIdentity) error { return nil }
func (s useTestSource) Resolve(context.Context, *pb.UseIdentity) (deviceuse.Resource, error) {
	return deviceuse.Resource{Key: "actual-disk-guid", Target: s.target, Group: useDiskGroup(s.target)}, nil
}

func useDriver(t *testing.T) (*Driver, *pb.UseIdentity) {
	t.Helper()
	scope := deviceuse.Scope{Driver: "disk.csi.azure.com", Namespace: "sources", NamespaceUID: "namespace-uid", Node: "node", NodeUID: "node-uid"}
	state, err := deviceuse.Initial(scope)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(state)
	target := "/subscriptions/sub/resourcegroups/rg/providers/microsoft.compute/disks/disk"
	d := &deviceuse.Domain{Scope: scope, Store: &useTestStore{data: data, scope: scope}, Match: strings.EqualFold,
		Resolver: useTestSource{target: target}}
	id := &pb.UseIdentity{UseId: "first", NodeId: "node", CompositeId: "compositedisk-" + strings.Repeat("a", 52) + ".cr",
		Source: &pb.SourceVolume{Driver: scope.Driver, VolumeHandle: target, PvName: "pv", PvUid: "pv-uid"}}
	return &Driver{useDomain: d}, id
}

func useCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if status.Code(err) != code {
		t.Fatalf("got %v, want %v: %v", status.Code(err), code, err)
	}
}

func TestDeviceUseGatesActualNativeDestructionAndAllowsBlockAttachment(t *testing.T) {
	driver, id := useDriver(t)
	ctx := t.Context()
	_, err := driver.useDomain.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	useCode(t, err, codes.OK)
	// No cloud client or mounter is installed: rejected calls cannot reach an effect.
	_, err = driver.ControllerUnpublishVolume(ctx, &csi.ControllerUnpublishVolumeRequest{VolumeId: id.Source.VolumeHandle, NodeId: "node"})
	useCode(t, err, codes.FailedPrecondition)
	_, err = driver.DeleteVolume(ctx, &csi.DeleteVolumeRequest{VolumeId: id.Source.VolumeHandle})
	useCode(t, err, codes.FailedPrecondition)
	_, err = driver.NodeUnpublishVolume(ctx, &csi.NodeUnpublishVolumeRequest{VolumeId: id.Source.VolumeHandle, TargetPath: "/private/block"})
	useCode(t, err, codes.FailedPrecondition)
	req := &csi.ControllerPublishVolumeRequest{VolumeId: id.Source.VolumeHandle, NodeId: "node",
		VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
			Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}, AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}}
	calls := 0
	_, err = runUseOperation(ctx, driver, req, "ControllerPublishVolume", func(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		calls++
		return &csi.ControllerPublishVolumeResponse{}, nil
	})
	useCode(t, err, codes.OK)
	if calls != 1 {
		t.Fatal("ordinary block attachment did not run under active use")
	}
	alias := proto.CloneOf(req)
	alias.VolumeId = "/subscriptions/sub/resourcegroups/another/providers/microsoft.compute/disks/disk"
	_, err = runUseOperation(ctx, driver, alias, "ControllerPublishVolume", func(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
		t.Fatal("ambiguous disk basename reached attachment")
		return nil, nil
	})
	useCode(t, err, codes.FailedPrecondition)
}

func TestDeviceUseCompletionPrecedesResponseAndOldReleaseCannotClearSuccessor(t *testing.T) {
	driver, id := useDriver(t)
	ctx := t.Context()
	_, err := driver.useDomain.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	useCode(t, err, codes.OK)
	next := proto.CloneOf(id)
	next.UseId = "successor"
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: id.Source.VolumeHandle, NodeId: "node"}
	_, err = runUseOperation(ctx, driver, req, "ControllerUnpublishVolume", func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
		_, err := driver.useDomain.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
		useCode(t, err, codes.Unavailable)
		return &csi.ControllerUnpublishVolumeResponse{}, nil
	})
	useCode(t, err, codes.OK)
	_, err = driver.useDomain.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	useCode(t, err, codes.OK)
	_, err = driver.useDomain.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	useCode(t, err, codes.OK)
	_, err = driver.ControllerUnpublishVolume(ctx, req)
	useCode(t, err, codes.FailedPrecondition)
}

func TestSameVMResourceIDAcceptsAzureVMSSInstanceAliases(t *testing.T) {
	const (
		base      = "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachineScaleSets/aks-cdh1380845e-22940400-vmss/virtualMachines/"
		canonical = base + "0"
		aliased   = base + "aks-cdh1380845e-22940400-vmss_0"
	)
	for _, test := range []struct {
		name     string
		actual   string
		expected string
		want     bool
	}{
		{name: "exact", actual: canonical, expected: canonical, want: true},
		{name: "case insensitive", actual: strings.ToUpper(canonical), expected: canonical, want: true},
		{name: "managed by alias", actual: aliased, expected: canonical, want: true},
		{name: "reverse alias", actual: canonical, expected: aliased, want: true},
		{name: "different instance", actual: base + "aks-cdh1380845e-22940400-vmss_1", expected: canonical},
		{name: "different scale set", actual: strings.Replace(aliased, "cdh1380845e", "other", 2), expected: canonical},
		{name: "different resource type", actual: "/subscriptions/sub/resourceGroups/rg/providers/Microsoft.Compute/virtualMachines/0", expected: canonical},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := sameVMResourceID(test.actual, test.expected); got != test.want {
				t.Fatalf("sameVMResourceID(%q, %q) = %t, want %t", test.actual, test.expected, got, test.want)
			}
		})
	}
}

func TestDeviceUseUnknownNativeOutcomeRetainsAdmissionAfterRelease(t *testing.T) {
	driver, id := useDriver(t)
	ctx := t.Context()
	req := &csi.ControllerUnpublishVolumeRequest{VolumeId: id.Source.VolumeHandle, NodeId: "node"}
	_, err := runUseOperation(ctx, driver, req, "ControllerUnpublishVolume", func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
		return nil, errors.New("native dispatch outcome unknown")
	})
	if err == nil {
		t.Fatal("unknown native outcome reported success")
	}
	_, err = driver.useDomain.ReleaseUse(ctx, &pb.ReleaseUseRequest{Identity: id})
	useCode(t, err, codes.OK)
	next := proto.CloneOf(id)
	next.UseId = "successor"
	_, err = driver.useDomain.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: next})
	useCode(t, err, codes.Unavailable)
	_, err = driver.ControllerUnpublishVolume(ctx, req)
	useCode(t, err, codes.Unavailable)
}

func TestDeviceUseNativeValidationCannotStrandAdmission(t *testing.T) {
	for _, missing := range []string{"volume-id", "stage-target", "capability"} {
		t.Run(missing, func(t *testing.T) {
			driver, id := useDriver(t)
			req := &csi.NodeStageVolumeRequest{
				VolumeId: id.Source.VolumeHandle, StagingTargetPath: "/private/stage",
				VolumeCapability: &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
					Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}, AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}}
			switch missing {
			case "volume-id":
				req.VolumeId = ""
			case "stage-target":
				req.StagingTargetPath = ""
			case "capability":
				req.VolumeCapability = nil
			}
			_, err := driver.NodeStageVolume(t.Context(), req)
			useCode(t, err, codes.InvalidArgument)
			_, err = driver.useDomain.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
			useCode(t, err, codes.OK)
		})
	}
}

func TestDeviceUseAllNativeWrappersPreflightRequiredInputs(t *testing.T) {
	const target = "/subscriptions/sub/resourcegroups/rg/providers/microsoft.compute/disks/disk"
	capability := &csi.VolumeCapability{AccessMode: &csi.VolumeCapability_AccessMode{
		Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER}, AccessType: &csi.VolumeCapability_Block{Block: &csi.VolumeCapability_BlockVolume{}}}
	for _, tc := range []struct {
		method string
		req    proto.Message
	}{
		{"CreateVolume", &csi.CreateVolumeRequest{Name: "new-volume"}},
		{"DeleteVolume", &csi.DeleteVolumeRequest{}},
		{"ControllerModifyVolume", &csi.ControllerModifyVolumeRequest{}},
		{"ControllerExpandVolume", &csi.ControllerExpandVolumeRequest{VolumeId: target}},
		{"ControllerPublishVolume", &csi.ControllerPublishVolumeRequest{VolumeId: target, VolumeCapability: capability}},
		{"ControllerUnpublishVolume", &csi.ControllerUnpublishVolumeRequest{VolumeId: target}},
		{"CreateSnapshot", &csi.CreateSnapshotRequest{SourceVolumeId: target}},
		{"NodeStageVolume", &csi.NodeStageVolumeRequest{VolumeId: target, VolumeCapability: capability}},
		{"NodePublishVolume", &csi.NodePublishVolumeRequest{VolumeId: target, StagingTargetPath: "/private/stage", VolumeCapability: capability}},
		{"NodeUnstageVolume", &csi.NodeUnstageVolumeRequest{VolumeId: target}},
		{"NodeUnpublishVolume", &csi.NodeUnpublishVolumeRequest{VolumeId: target}},
		{"NodeExpandVolume", &csi.NodeExpandVolumeRequest{VolumeId: target}},
	} {
		t.Run(tc.method, func(t *testing.T) {
			driver, id := useDriver(t)
			called := false
			_, err := runUseOperation(t.Context(), driver, tc.req, tc.method,
				func(context.Context, proto.Message) (*csi.NodeStageVolumeResponse, error) {
					called = true
					return nil, status.Error(codes.InvalidArgument, "native validation rejected request")
				})
			useCode(t, err, codes.InvalidArgument)
			if called {
				t.Fatal("known invalid request entered native operation")
			}
			_, err = driver.useDomain.AcquireUse(t.Context(), &pb.AcquireUseRequest{Identity: id})
			useCode(t, err, codes.OK)
		})
	}
}

func TestDeviceUsePostDispatchInvalidArgumentStillRetainsAdmission(t *testing.T) {
	driver, id := useDriver(t)
	ctx := t.Context()
	dispatched := false
	_, err := runUseOperation(ctx, driver, &csi.ControllerUnpublishVolumeRequest{
		VolumeId: id.Source.VolumeHandle, NodeId: "node"}, "ControllerUnpublishVolume",
		func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
			dispatched = true
			return nil, status.Error(codes.InvalidArgument, "error returned after a native effect")
		})
	useCode(t, err, codes.InvalidArgument)
	if !dispatched {
		t.Fatal("test never reached the physical-effect boundary")
	}
	_, err = driver.useDomain.AcquireUse(ctx, &pb.AcquireUseRequest{Identity: id})
	useCode(t, err, codes.Unavailable)
}
