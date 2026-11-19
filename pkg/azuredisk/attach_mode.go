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
	"fmt"
	"strings"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	consts "sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
)

// Ordinary CSI has no origin owner credentials. Hold its admission across the
// entire call, including QAD and the controller's dangling-attach recovery.
func (d *Driver) guardDisk(ctx context.Context, rpc proto.Message, diskURI string) (func(), error) {
	if d.publishedBlock.Matches(diskURI) {
		if d.origin != nil {
			return nil, fmt.Errorf("cannot combine origin and published block modes")
		}
		switch q := rpc.(type) {
		case *csi.ControllerUnpublishVolumeRequest:
			if q.NodeId != d.publishedBlock.Selection().NodeID {
				return nil, status.Error(codes.FailedPrecondition, "wrong node for selected disk teardown")
			}
		case *csi.NodeUnpublishVolumeRequest, *csi.NodeUnstageVolumeRequest:
			if d.NodeID != d.publishedBlock.Selection().NodeID {
				return nil, status.Error(codes.FailedPrecondition, "wrong node for selected disk teardown")
			}
		case *csi.DeleteVolumeRequest:
		default:
			return nil, status.Error(codes.FailedPrecondition, "operation not supported for selected block disk")
		}
		if err := d.publishedBlock.EnterTeardown(ctx); err != nil {
			return nil, err
		}
		return func() {}, nil
	}
	if d.publishedBlock != nil {
		switch rpc.(type) {
		case *csi.ControllerPublishVolumeRequest, *csi.ControllerUnpublishVolumeRequest:
			// These paths can enter VM helpers that also match by basename.
			// Disk-RP operations use the full ARM ID and do not share this
			// ambiguity across resource groups or subscriptions.
			id := d.publishedBlock.Selection().ResourceID
			if strings.EqualFold(diskURI[strings.LastIndexByte(diskURI, '/')+1:], id[strings.LastIndexByte(id, '/')+1:]) {
				return nil, status.Error(codes.FailedPrecondition, "ambiguous selected disk basename")
			}
		}
	}
	if d.origin == nil {
		return func() {}, nil
	}
	return d.origin.BeginCSI(ctx, rpc, diskURI)
}

// getAttachMode returns the requested attachment architecture.
func getAttachMode(parameters map[string]string) (string, error) {
	mode, exists := azureutils.ParseDiskParametersForKey(parameters, consts.AttachModeField)
	if !exists {
		return consts.AttachModeControllerDriven, nil
	}

	switch {
	case strings.EqualFold(mode, consts.AttachModeControllerDriven):
		return consts.AttachModeControllerDriven, nil
	case strings.EqualFold(mode, consts.AttachModeNodeDriven):
		return consts.AttachModeNodeDriven, nil
	default:
		return "", fmt.Errorf("unsupported %s %q: supported values are %s and %s", consts.AttachModeField, mode, consts.AttachModeControllerDriven, consts.AttachModeNodeDriven)
	}
}
