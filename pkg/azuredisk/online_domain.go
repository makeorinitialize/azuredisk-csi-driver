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

package azuredisk

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/devicelease"
)

type onlineDomainConfig struct {
	Identity         devicelease.OnlineIdentity `json:"identity"`
	JournalNamespace string                     `json:"journalNamespace"`
	JournalName      string                     `json:"journalName"`
	JournalUID       types.UID                  `json:"journalUID"`
	Endpoint         string                     `json:"endpoint"`
	ServerCertFile   string                     `json:"serverCertFile"`
	ServerKeyFile    string                     `json:"serverKeyFile"`
	ClientCAFile     string                     `json:"clientCAFile"`
	Grants           []devicelease.OnlineGrant  `json:"grants"`
}

func (d *Driver) configureOnline(ctx context.Context) (*onlineDomainConfig, error) {
	if d.onlineConfigPath == "" {
		return nil, nil
	}
	if d.onlineRESTConfig == nil || d.kubeClient == nil || d.diskController == nil || d.origin != nil || d.publishedBlock != nil {
		return nil, fmt.Errorf("online canary needs Kubernetes/ARM clients and cannot combine origin modes")
	}
	info, err := os.Stat(d.onlineConfigPath)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0022 != 0 || info.Size() > 128*1024 {
		return nil, fmt.Errorf("online config must be a protected regular file")
	}
	f, err := os.Open(d.onlineConfigPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var cfg onlineDomainConfig
	decoder := json.NewDecoder(io.LimitReader(f, 128*1024))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("trailing online configuration")
	}
	if cfg.Identity.Backing.EffectiveOriginDriver() != d.Name {
		return nil, fmt.Errorf("online originDriver must exactly match this process --drivername")
	}
	if d.NodeID != "" && d.NodeID != cfg.Identity.Backing.NodeName {
		return nil, fmt.Errorf("online canary is restricted to its pinned node")
	}
	if cfg.JournalNamespace == "" || cfg.Endpoint == "" {
		return nil, fmt.Errorf("journal namespace and authenticated endpoint required")
	}
	dyn, err := dynamic.NewForConfig(d.onlineRESTConfig)
	if err != nil {
		return nil, err
	}
	domain, err := devicelease.NewOnlineDomain(cfg.Identity, d.kubeClient.CoreV1().ConfigMaps(cfg.JournalNamespace), cfg.JournalName, cfg.JournalUID, d.kubeClient, dyn)
	if err != nil {
		return nil, err
	}
	if err := domain.Check(ctx); err != nil {
		return nil, err
	}
	d.onlineDomain, d.diskController.onlineDomain = domain, domain
	return &cfg, nil
}

func (d *Driver) startOnline(ctx context.Context, cfg *onlineDomainConfig, native *grpc.Server) (func(), error) {
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
		return nil, fmt.Errorf("invalid online client roots")
	}
	server, err := devicelease.NewOnlineServer(d.onlineDomain, d.observeRetainedBlock, cert, roots, cfg.Grants)
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", cfg.Endpoint)
	if err != nil {
		return nil, err
	}
	go func() {
		_ = server.Serve(listener)
		native.Stop() // No silent serving with a dead provider endpoint.
	}()
	stop := func() { server.Stop(); _ = listener.Close() }
	return stop, nil
}

type onlineHoldContextKey struct{}

func (d *Driver) publishedHold(ctx context.Context) *devicelease.BlockHold {
	if d.onlineDomain != nil {
		h, _ := ctx.Value(onlineHoldContextKey{}).(*devicelease.BlockHold)
		return h
	}
	return d.publishedBlock
}

func (d *Driver) onlineNative(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if d.onlineDomain == nil {
		if d.onlineConfigPath != "" {
			return nil, status.Error(codes.Unavailable, "configured online admission is not initialized")
		}
		return next(ctx, req)
	}
	method := info.FullMethod[strings.LastIndexByte(info.FullMethod, '/')+1:]
	switch method {
	case "CreateVolume", "DeleteVolume", "ControllerPublishVolume", "ControllerUnpublishVolume", "ControllerModifyVolume", "ControllerExpandVolume",
		"NodeStageVolume", "NodePublishVolume", "NodeUnstageVolume", "NodeUnpublishVolume", "NodeExpandVolume", "CreateSnapshot":
	default:
		return next(ctx, req)
	}
	message, ok := req.(proto.Message)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "invalid native request")
	}
	wire, err := (proto.MarshalOptions{Deterministic: true}).Marshal(message)
	if err != nil || len(wire) > 64*1024 {
		return nil, status.Error(codes.InvalidArgument, "native request not journalable")
	}
	hash := sha256.Sum256(append([]byte(method+"\x00"), wire...))
	digest := hex.EncodeToString(hash[:])
	var id string
	if q, ok := req.(interface{ GetVolumeId() string }); ok {
		id = q.GetVolumeId()
	}
	if q, ok := req.(*csi.CreateSnapshotRequest); ok {
		id = q.GetSourceVolumeId()
	}
	ctx, hold, err := d.onlineDomain.BeginNative(ctx, method, digest, id, "", "")
	if err != nil {
		return nil, err
	}
	if hold != nil {
		ctx = context.WithValue(ctx, onlineHoldContextKey{}, hold)
	}
	out, callErr := next(ctx, req)
	var outcome []byte
	if callErr == nil {
		result, ok := out.(proto.Message)
		if !ok || result == nil || !result.ProtoReflect().IsValid() {
			return nil, status.Error(codes.Unavailable, "native result cannot be bound to completion")
		}
		outcome, err = (proto.MarshalOptions{Deterministic: true}).Marshal(result)
		if err != nil {
			return nil, status.Error(codes.Unavailable, "native result serialization failed")
		}
	}
	if endErr := d.onlineDomain.EndNative(ctx, callErr == nil, "", outcome); endErr != nil {
		return nil, endErr
	}
	return out, callErr
}
