// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package azuredisk

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/compute/armcompute/v7"
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse"
	azcache "sigs.k8s.io/cloud-provider-azure/pkg/cache"
)

type deviceUseConfig struct {
	Scope            deviceuse.Scope   `json:"scope"`
	JournalNamespace string            `json:"journalNamespace"`
	JournalName      string            `json:"journalName"`
	JournalUID       types.UID         `json:"journalUID"`
	Endpoint         string            `json:"endpoint"`
	ServerCertFile   string            `json:"serverCertFile"`
	ServerKeyFile    string            `json:"serverKeyFile"`
	ClientCAFile     string            `json:"clientCAFile"`
	Grants           []deviceuse.Grant `json:"grants"`
}

func (d *Driver) configureUse(ctx context.Context) (*deviceUseConfig, error) {
	if d.useConfigPath == "" {
		return nil, nil
	}
	if d.useRESTConfig == nil || d.kubeClient == nil || d.diskController == nil {
		return nil, fmt.Errorf("device-use gates require independent Kubernetes and Azure clients")
	}
	info, err := os.Stat(d.useConfigPath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() > 128*1024 {
		return nil, fmt.Errorf("device-use config must be a protected regular file")
	}
	f, err := os.Open(d.useConfigPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg deviceUseConfig
	decoder := json.NewDecoder(io.LimitReader(f, 128*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing device-use configuration")
	}
	if !cfg.Scope.Valid() || cfg.Scope.Driver != d.Name || d.NodeID != "" && d.NodeID != cfg.Scope.Node ||
		cfg.JournalNamespace == "" || cfg.JournalName == "" || cfg.JournalUID == "" || cfg.Endpoint == "" {
		return nil, fmt.Errorf("exact driver/node/namespace and pinned journal required")
	}
	if _, err := deviceuse.Authorize(cfg.Scope, cfg.Grants); err != nil {
		return nil, err
	}
	store := deviceuse.ConfigMapStore{Scope: cfg.Scope, Client: d.kubeClient.CoreV1().ConfigMaps(cfg.JournalNamespace),
		Name: cfg.JournalName, UID: cfg.JournalUID}
	if err := store.Transact(ctx, func(*deviceuse.Snapshot) (bool, error) { return false, nil }); err != nil {
		return nil, err
	}
	domain := &deviceuse.Domain{Scope: cfg.Scope, Store: store, Match: strings.EqualFold,
		Resolver: deviceuse.KubernetesResolver{Scope: cfg.Scope, Client: d.kubeClient, Native: d.observeUseSource}}
	d.useDomain, d.diskController.useDomain = domain, domain
	return &cfg, nil
}

func (d *Driver) startUse(_ context.Context, cfg *deviceUseConfig, native *grpc.Server) (func(), error) {
	if cfg == nil || d.NodeID != "" {
		return func() {}, nil
	}
	cert, err := tls.LoadX509KeyPair(cfg.ServerCertFile, cfg.ServerKeyFile)
	if err != nil {
		return nil, err
	}
	ca, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, err
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, fmt.Errorf("invalid device-use client roots")
	}
	server, err := deviceuse.NewServer(d.useDomain, cert, roots, cfg.Grants)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	go func() {
		_ = server.Serve(listener)
		native.Stop()
	}()
	return func() { server.Stop(); _ = listener.Close() }, nil
}

func useDiskGroup(id string) string {
	return strings.ToLower(id[strings.LastIndexByte(id, '/')+1:])
}

func runUseOperation[Q proto.Message, R any](ctx context.Context, d *Driver, req Q, method string, next func(context.Context, Q) (*R, error)) (*R, error) {
	if d.useDomain == nil {
		if d.useConfigPath != "" {
			return nil, status.Error(codes.Unavailable, "configured device-use admission is not initialized")
		}
		return next(ctx, req)
	}
	if err := validateUseNativeRequest(req); err != nil {
		return nil, err
	}
	var target string
	if q, ok := any(req).(interface{ GetVolumeId() string }); ok {
		target = q.GetVolumeId()
	}
	if q, ok := any(req).(*csi.CreateSnapshotRequest); ok {
		target = q.GetSourceVolumeId()
	}
	var finish func(bool) error
	var err error
	switch method {
	case "ControllerPublishVolume", "NodeStageVolume", "NodePublishVolume":
		q, ok := any(req).(interface{ GetVolumeCapability() *csi.VolumeCapability })
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "native preparation capability required")
		}
		node := d.NodeID
		group := ""
		if q, ok := any(req).(*csi.ControllerPublishVolumeRequest); ok {
			node = q.GetNodeId()
			group = useDiskGroup(target)
		}
		ctx, finish, err = d.useDomain.BeginPreparationInGroup(ctx, method, target, group, node, q.GetVolumeCapability().GetBlock() != nil)
	default:
		group := ""
		if method == "ControllerUnpublishVolume" {
			group = useDiskGroup(target)
		}
		if q, ok := any(req).(*csi.CreateVolumeRequest); ok {
			target = "create:" + q.GetName()
		}
		ctx, finish, err = d.useDomain.BeginNative(ctx, method, target, group)
	}
	if err != nil {
		return nil, err
	}
	out, callErr := next(ctx, req)
	if endErr := finish(callErr == nil); endErr != nil {
		return nil, errors.Join(callErr, endErr)
	}
	return out, callErr
}

// These checks are pure request validation from the native handlers. Once a
// handler runs, its error code never proves that physical effects did not occur.
func validateUseNativeRequest(req proto.Message) error {
	invalid := func(message string) error { return status.Error(codes.InvalidArgument, message) }
	if q, ok := req.(interface{ GetVolumeId() string }); ok && q.GetVolumeId() == "" {
		return invalid("Volume ID not provided")
	}
	validateCapability := func(capability *csi.VolumeCapability, parameters map[string]string) error {
		if capability == nil {
			return invalid("Volume capability not provided")
		}
		shares, err := azureutils.GetMaxShares(parameters)
		if err != nil {
			return invalid("MaxShares value not supported")
		}
		if err := azureutils.IsValidVolumeCapabilities([]*csi.VolumeCapability{capability}, shares); err != nil {
			return invalid(err.Error())
		}
		return nil
	}
	switch q := req.(type) {
	case *csi.NodeStageVolumeRequest:
		if q.GetStagingTargetPath() == "" {
			return invalid("Staging target not provided")
		}
		if err := validateCapability(q.GetVolumeCapability(), q.GetVolumeContext()); err != nil {
			return err
		}
		if _, _, err := resolveFSType(q.GetVolumeCapability(), q.GetVolumeContext()); err != nil {
			return invalid(err.Error())
		}
	case *csi.NodePublishVolumeRequest:
		if err := validateCapability(q.GetVolumeCapability(), q.GetVolumeContext()); err != nil {
			return err
		}
		if q.GetStagingTargetPath() == "" {
			return invalid("Staging target not provided")
		}
		if q.GetTargetPath() == "" {
			return invalid("Target path not provided")
		}
	case *csi.NodeUnstageVolumeRequest:
		if q.GetStagingTargetPath() == "" {
			return invalid("Staging target not provided")
		}
	case *csi.NodeUnpublishVolumeRequest:
		if q.GetTargetPath() == "" {
			return invalid("Target path not provided")
		}
	case *csi.NodeExpandVolumeRequest:
		if q.GetVolumePath() == "" {
			return invalid("volume path must be provided")
		}
	case *csi.ControllerPublishVolumeRequest:
		if q.GetNodeId() == "" {
			return invalid("Node ID not provided")
		}
		if err := validateCapability(q.GetVolumeCapability(), q.GetVolumeContext()); err != nil {
			return err
		}
		if _, err := getAttachMode(q.GetVolumeContext()); err != nil {
			return invalid(err.Error())
		}
		if _, _, _, err := azureutils.GetInfoFromURI(q.GetVolumeId()); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
	case *csi.ControllerUnpublishVolumeRequest:
		if q.GetNodeId() == "" {
			return invalid("Node ID not provided")
		}
		if _, _, _, err := azureutils.GetInfoFromURI(q.GetVolumeId()); err != nil {
			return status.Errorf(codes.Internal, "%v", err)
		}
	case *csi.ControllerExpandVolumeRequest:
		if q.GetCapacityRange().GetRequiredBytes() == 0 {
			return invalid("volume capacity range missing in request")
		}
	case *csi.ControllerModifyVolumeRequest:
		parameters, err := normalizeMutableDiskParameters(q.GetMutableParameters())
		if err != nil {
			return invalid("Invalid mutable disk parameters: " + err.Error())
		}
		if _, err := azureutils.ParseDiskParameters(parameters); err != nil {
			return invalid("Failed parsing disk parameters: " + err.Error())
		}
	case *csi.CreateVolumeRequest:
		if q.GetName() == "" {
			return invalid("CreateVolume Name must be provided")
		}
		if len(q.GetVolumeCapabilities()) == 0 {
			return invalid("CreateVolume Volume capabilities must be provided")
		}
		mutable, err := normalizeMutableDiskParameters(q.GetMutableParameters())
		if err != nil {
			return invalid("Invalid mutable disk parameters: " + err.Error())
		}
		parameters := make(map[string]string, len(q.GetParameters())+len(mutable))
		for key, value := range q.GetParameters() {
			if _, overridden := mutable[mutableDiskParameterKey(key)]; !overridden {
				parameters[key] = value
			}
		}
		for key, value := range mutable {
			parameters[key] = value
		}
		disk, err := azureutils.ParseDiskParameters(parameters)
		if err != nil {
			return invalid("Failed parsing disk parameters: " + err.Error())
		}
		if _, err := getAttachMode(parameters); err != nil {
			return invalid("Failed parsing disk parameters: " + err.Error())
		}
		if err := azureutils.IsValidVolumeCapabilities(q.GetVolumeCapabilities(), disk.MaxShares); err != nil {
			return invalid(err.Error())
		}
	case *csi.CreateSnapshotRequest:
		if q.GetSourceVolumeId() == "" {
			return invalid("CreateSnapshot Source Volume ID must be provided")
		}
		if q.GetName() == "" {
			return invalid("snapshot name must be provided")
		}
	}
	return nil
}

func (d *Driver) observeUseSource(ctx context.Context, pv *core.PersistentVolume, node *core.Node) (deviceuse.Resource, error) {
	bad := status.Error(codes.FailedPrecondition, "independent existing Azure disk/VM identity is not stable")
	if d.cloud == nil || d.diskController == nil || d.clientFactory == nil {
		return deviceuse.Resource{}, status.Error(codes.Unavailable, "independent Azure observer unavailable")
	}
	mode, modeErr := getAttachMode(pv.Spec.CSI.VolumeAttributes)
	shares, sharesErr := azureutils.GetMaxShares(pv.Spec.CSI.VolumeAttributes)
	if modeErr != nil || mode != azureconstants.AttachModeControllerDriven || sharesErr != nil || shares != 1 || d.hasQADInfo(pv) {
		return deviceuse.Resource{}, bad
	}
	disk, err := d.diskController.GetDiskByURI(ctx, pv.Spec.CSI.VolumeHandle)
	if err != nil {
		return deviceuse.Resource{}, err
	}
	if disk == nil || disk.ID == nil || !strings.EqualFold(*disk.ID, pv.Spec.CSI.VolumeHandle) || disk.Properties == nil ||
		ptr.Deref(disk.Properties.UniqueID, "") == "" || ptr.Deref(disk.Properties.MaxShares, 1) != 1 ||
		len(disk.ManagedByExtended) != 0 || len(disk.Properties.ShareInfo) != 0 ||
		ptr.Deref(disk.Properties.ProvisioningState, "") != "Succeeded" || disk.Properties.PropertyUpdatesInProgress != nil ||
		disk.Properties.DiskState == nil {
		return deviceuse.Resource{}, bad
	}
	set, err := d.cloud.GetNodeVMSet(ctx, types.NodeName(node.Name), azcache.CacheReadTypeForceRefresh)
	if err != nil {
		return deviceuse.Resource{}, err
	}
	vmID, err := set.GetInstanceIDByNodeName(ctx, node.Name)
	if err != nil {
		return deviceuse.Resource{}, err
	}
	p := strings.Split(vmID, "/")
	if (len(p) != 9 && len(p) != 11) || p[0] != "" || !strings.EqualFold(p[1], "subscriptions") ||
		!strings.EqualFold(p[2], d.cloud.SubscriptionID) || !strings.EqualFold(p[3], "resourceGroups") ||
		!strings.EqualFold(p[5], "providers") || !strings.EqualFold(p[6], "Microsoft.Compute") {
		return deviceuse.Resource{}, bad
	}
	var actual, guid, state, computer *string
	var storage *armcompute.StorageProfile
	switch {
	case len(p) == 9 && strings.EqualFold(p[7], "virtualMachines"):
		vm, getErr := d.clientFactory.GetVirtualMachineClient().Get(ctx, p[4], p[8], nil)
		if getErr != nil {
			return deviceuse.Resource{}, getErr
		}
		if vm != nil && vm.Properties != nil && vm.Properties.OSProfile != nil {
			actual, guid, state, computer, storage = vm.ID, vm.Properties.VMID, vm.Properties.ProvisioningState, vm.Properties.OSProfile.ComputerName, vm.Properties.StorageProfile
		}
	case len(p) == 11 && strings.EqualFold(p[7], "virtualMachineScaleSets") && strings.EqualFold(p[9], "virtualMachines"):
		vm, getErr := d.clientFactory.GetVirtualMachineScaleSetVMClient().Get(ctx, p[4], p[8], p[10])
		if getErr != nil {
			return deviceuse.Resource{}, getErr
		}
		if vm != nil && vm.Properties != nil && vm.Properties.OSProfile != nil {
			actual, guid, state, computer, storage = vm.ID, vm.Properties.VMID, vm.Properties.ProvisioningState, vm.Properties.OSProfile.ComputerName, vm.Properties.StorageProfile
		}
	}
	if !strings.EqualFold(ptr.Deref(actual, ""), vmID) || ptr.Deref(guid, "") == "" ||
		ptr.Deref(state, "") != "Succeeded" || ptr.Deref(computer, "") != node.Name || storage == nil {
		return deviceuse.Resource{}, bad
	}
	present := 0
	for _, attached := range storage.DataDisks {
		if attached != nil && attached.ManagedDisk != nil && strings.EqualFold(ptr.Deref(attached.ManagedDisk.ID, ""), *disk.ID) {
			if ptr.Deref(attached.ToBeDetached, false) {
				return deviceuse.Resource{}, bad
			}
			present++
		}
	}
	switch *disk.Properties.DiskState {
	case armcompute.DiskStateUnattached:
		if disk.ManagedBy != nil || present != 0 {
			return deviceuse.Resource{}, bad
		}
	case armcompute.DiskStateAttached:
		if !sameVMResourceID(ptr.Deref(disk.ManagedBy, ""), vmID) || present != 1 {
			return deviceuse.Resource{}, bad
		}
	default:
		return deviceuse.Resource{}, bad
	}
	return deviceuse.Resource{Key: *disk.Properties.UniqueID, Target: strings.ToLower(*disk.ID), Group: useDiskGroup(*disk.ID)}, nil
}

func sameVMResourceID(actual, expected string) bool {
	if strings.EqualFold(actual, expected) {
		return true
	}
	a := strings.Split(strings.Trim(actual, "/"), "/")
	b := strings.Split(strings.Trim(expected, "/"), "/")
	if len(a) != 10 || len(b) != len(a) {
		return false
	}
	for i := 0; i < len(a)-1; i++ {
		if !strings.EqualFold(a[i], b[i]) {
			return false
		}
	}
	if !strings.EqualFold(a[6], "virtualMachineScaleSets") || !strings.EqualFold(a[8], "virtualMachines") {
		return false
	}
	aliased := a[7] + "_" + b[9]
	reversed := b[7] + "_" + a[9]
	return strings.EqualFold(a[9], aliased) || strings.EqualFold(b[9], reversed)
}
