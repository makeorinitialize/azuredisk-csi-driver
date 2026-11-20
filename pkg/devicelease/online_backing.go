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
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureconstants"
	"sigs.k8s.io/azuredisk-csi-driver/pkg/azureutils"
)

const (
	backingPrefix           = "dmstack.csi.infinitydb.io/"
	backingJournalKey       = backingPrefix + "external-source-pvcs-v1"
	legacyBackingJournalKey = backingPrefix + "backing-pvcs-v1"
)

var (
	compositeGVR  = schema.GroupVersionResource{Group: "storage.infinitydb.io", Version: "v1alpha1", Resource: "compositevolumes"}
	compositeName = regexp.MustCompile(`^dmstack-[a-z2-7]{52}$`)
	compositeUID  = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,66}$`)
)

// Field order is the facade's pinned external-source-pvcs-v1 contract.
type externalScope struct {
	Namespace    string    `json:"namespace"`
	NamespaceUID types.UID `json:"namespaceUID"`
	NodeName     string    `json:"nodeName"`
	NodeUID      types.UID `json:"nodeUID"`
	OriginDriver string    `json:"originDriver,omitempty"`
	CacheDriver  string    `json:"cacheDriver,omitempty"`
}

func (s externalScope) backing() BackingConfig {
	return BackingConfig{Namespace: s.Namespace, NamespaceUID: s.NamespaceUID, NodeName: s.NodeName, NodeUID: s.NodeUID,
		OriginDriver: s.OriginDriver, CacheDriver: s.CacheDriver}
}

func (s externalScope) matches(c BackingConfig) bool {
	c.OriginStorageClass, c.CacheStorageClass, c.OriginBytes, c.CacheBytes = "", "", 0, 0
	return s.backing().same(c)
}

type outerClaim struct {
	Name string    `json:"name"`
	UID  types.UID `json:"uid"`
}

type backingClaim struct {
	Name      string    `json:"name"`
	UID       types.UID `json:"uid"`
	PVName    string    `json:"pvName"`
	PVUID     types.UID `json:"pvUID"`
	ClassName string    `json:"className"`
	ClassUID  types.UID `json:"classUID"`
	Driver    string    `json:"driver"`
	Handle    string    `json:"handle"`
	Bytes     int64     `json:"bytes"`
}

type backingJournal struct {
	Version int           `json:"version"`
	Owner   string        `json:"owner"`
	Config  externalScope `json:"config"`
	Outer   outerClaim    `json:"outer"`
	Origin  backingClaim  `json:"origin"`
	Cache   backingClaim  `json:"cache"`
}

func (j backingJournal) same(other backingJournal) bool {
	return j.Version == other.Version && j.Owner == other.Owner && j.Config.backing().same(other.Config.backing()) &&
		j.Outer == other.Outer && j.Origin == other.Origin && j.Cache == other.Cache
}

type backingProof struct {
	Journal                   backingJournal
	RequestID, Revision, VMID string
}

func backingIdentityError() error {
	return status.Error(codes.FailedPrecondition, "external source journal or live Kubernetes identity/specification mismatch")
}

func (d *OnlineDomain) checkDomainObjects(ctx context.Context) error {
	crd, err := d.dynamic.Resource(schema.GroupVersionResource{Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions"}).
		Get(ctx, "compositevolumes.storage.infinitydb.io", metav1.GetOptions{})
	if err != nil {
		return unavailable(err)
	}
	scope, _, _ := unstructured.NestedString(crd.Object, "spec", "scope")
	group, _, _ := unstructured.NestedString(crd.Object, "spec", "group")
	plural, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "plural")
	kind, _, _ := unstructured.NestedString(crd.Object, "spec", "names", "kind")
	if crd.GetUID() != d.identity.CompositeCRDUID || crd.GetDeletionTimestamp() != nil || scope != "Cluster" || group != compositeGVR.Group ||
		plural != compositeGVR.Resource || kind != "CompositeVolume" {
		return backingIdentityError()
	}
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, v := range versions {
		m, ok := v.(map[string]any)
		if ok && m["name"] == compositeGVR.Version && m["served"] == true {
			return nil
		}
	}
	return backingIdentityError()
}

func (d *OnlineDomain) checkScope(ctx context.Context) (*core.Namespace, *core.Node, error) {
	c := d.identity.Backing
	ns, err := d.kube.CoreV1().Namespaces().Get(ctx, c.Namespace, metav1.GetOptions{})
	if err != nil {
		return nil, nil, unavailable(err)
	}
	node, err := d.kube.CoreV1().Nodes().Get(ctx, c.NodeName, metav1.GetOptions{})
	if err != nil {
		return nil, nil, unavailable(err)
	}
	if ns.UID != c.NamespaceUID || ns.DeletionTimestamp != nil || node.UID != c.NodeUID || node.DeletionTimestamp != nil ||
		node.Spec.Unschedulable || !strings.HasPrefix(node.Spec.ProviderID, "azure://") {
		return nil, nil, backingIdentityError()
	}
	return ns, node, nil
}

func (d *OnlineDomain) resolveBacking(ctx context.Context, id string) (backingProof, error) {
	var out backingProof
	if !compositeName.MatchString(id) {
		return out, backingIdentityError()
	}
	if err := d.checkDomainObjects(ctx); err != nil {
		return out, err
	}
	v, err := d.dynamic.Resource(compositeGVR).Get(ctx, id, metav1.GetOptions{})
	if err != nil {
		return out, unavailable(err)
	}
	if v.GetAPIVersion() != "storage.infinitydb.io/v1alpha1" || v.GetKind() != "CompositeVolume" || v.GetName() != id ||
		v.GetNamespace() != "" || !compositeUID.MatchString(string(v.GetUID())) || v.GetResourceVersion() == "" || v.GetDeletionTimestamp() != nil {
		return out, backingIdentityError()
	}
	if _, old := v.GetAnnotations()[legacyBackingJournalKey]; old {
		return out, backingIdentityError()
	}
	raw := v.GetAnnotations()[backingJournalKey]
	if json.Unmarshal([]byte(raw), &out.Journal) != nil {
		return out, backingIdentityError()
	}
	j := out.Journal
	canonical, _ := json.Marshal(j)
	c := d.identity.Backing
	originSize, _, _ := unstructured.NestedInt64(v.Object, "spec", "capacity")
	cacheSize, _, _ := unstructured.NestedInt64(v.Object, "spec", "cacheBytes")
	provider, _, _ := unstructured.NestedString(v.Object, "spec", "originParameters", "provider")
	pool, _, _ := unstructured.NestedString(v.Object, "spec", "originParameters", "poolID")
	request, _, _ := unstructured.NestedString(v.Object, "spec", "originParameters", "requestID")
	if string(canonical) != raw || j.Version != 1 || j.Owner != id+"."+string(v.GetUID()) || !j.Config.matches(c) ||
		v.GetAnnotations()[backingPrefix+"selected-node"] != c.NodeName || originSize != j.Origin.Bytes || cacheSize != j.Cache.Bytes ||
		j.Origin.Bytes <= 0 || j.Cache.Bytes <= 0 || provider != d.identity.ProviderID || pool != d.identity.PoolID || !validID(request) ||
		j.Outer.UID == "" || len(validation.IsDNS1123Subdomain(j.Outer.Name)) != 0 ||
		j.Origin.Name == j.Cache.Name || j.Outer.Name == j.Origin.Name || j.Outer.Name == j.Cache.Name {
		return out, backingIdentityError()
	}
	ns, node, err := d.checkScope(ctx)
	if err != nil {
		return out, err
	}
	outer, err := d.kube.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, j.Outer.Name, metav1.GetOptions{})
	if err != nil {
		return out, unavailable(err)
	}
	outerSize := outer.Spec.Resources.Requests[core.ResourceStorage]
	if outer.UID != j.Outer.UID || outer.DeletionTimestamp != nil || outer.ResourceVersion == "" ||
		outer.Annotations[backingPrefix+"origin-pvc"] != j.Origin.Name || outer.Annotations[backingPrefix+"cache-pvc"] != j.Cache.Name ||
		outer.Spec.VolumeMode != nil && *outer.Spec.VolumeMode != core.PersistentVolumeFilesystem ||
		!slices.Equal(outer.Spec.AccessModes, []core.PersistentVolumeAccessMode{core.ReadWriteOnce}) ||
		outerSize.Cmp(*resource.NewQuantity(j.Origin.Bytes, resource.BinarySI)) != 0 {
		return out, backingIdentityError()
	}
	revisions := []string{v.GetResourceVersion(), ns.ResourceVersion, node.ResourceVersion, outer.ResourceVersion}
	for _, side := range []struct {
		pin           backingClaim
		driver, class string
		bytes         int64
	}{
		{j.Origin, c.EffectiveOriginDriver(), c.OriginStorageClass, c.OriginBytes},
		{j.Cache, c.EffectiveCacheDriver(), c.CacheStorageClass, c.CacheBytes},
	} {
		pin := side.pin
		if len(validation.IsDNS1123Subdomain(pin.Name)) != 0 || pin.UID == "" || pin.PVName == "" || pin.PVUID == "" ||
			pin.ClassName == "" || pin.ClassUID == "" || pin.Driver != side.driver || pin.Handle == "" || pin.Bytes <= 0 || pin.Bytes%512 != 0 ||
			side.class != "" && pin.ClassName != side.class || side.bytes != 0 && pin.Bytes != side.bytes {
			return out, backingIdentityError()
		}
		sc, err := d.kube.StorageV1().StorageClasses().Get(ctx, pin.ClassName, metav1.GetOptions{})
		if err != nil {
			return out, unavailable(err)
		}
		if sc.UID != pin.ClassUID || sc.DeletionTimestamp != nil || sc.Provisioner != pin.Driver || !backingTopology(sc.AllowedTopologies, node) {
			return out, backingIdentityError()
		}
		pvc, err := d.kube.CoreV1().PersistentVolumeClaims(c.Namespace).Get(ctx, pin.Name, metav1.GetOptions{})
		if err != nil {
			return out, unavailable(err)
		}
		requested := pvc.Spec.Resources.Requests[core.ResourceStorage]
		if pvc.UID != pin.UID || pvc.DeletionTimestamp != nil || pvc.ResourceVersion == "" || pvc.Status.Phase != core.ClaimBound ||
			pvc.Spec.VolumeName != pin.PVName || pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName != pin.ClassName ||
			pvc.Spec.VolumeMode == nil || *pvc.Spec.VolumeMode != core.PersistentVolumeBlock ||
			!slices.Equal(pvc.Spec.AccessModes, []core.PersistentVolumeAccessMode{core.ReadWriteOnce}) ||
			requested.Cmp(*resource.NewQuantity(pin.Bytes, resource.BinarySI)) != 0 {
			return out, backingIdentityError()
		}
		pv, err := d.kube.CoreV1().PersistentVolumes().Get(ctx, pin.PVName, metav1.GetOptions{})
		if err != nil {
			return out, unavailable(err)
		}
		capacity, claimCapacity := pv.Spec.Capacity[core.ResourceStorage], pvc.Status.Capacity[core.ResourceStorage]
		if pv.UID != pin.PVUID || pv.DeletionTimestamp != nil || pv.Status.Phase != core.VolumeBound ||
			pv.Spec.ClaimRef == nil || pv.Spec.ClaimRef.UID != pin.UID || pv.Spec.ClaimRef.Name != pin.Name || pv.Spec.ClaimRef.Namespace != c.Namespace ||
			pv.Spec.CSI == nil || pv.Spec.CSI.Driver != pin.Driver || pv.Spec.CSI.VolumeHandle != pin.Handle || pv.Spec.CSI.ReadOnly ||
			pv.Spec.StorageClassName != pin.ClassName || pv.Spec.VolumeMode == nil || *pv.Spec.VolumeMode != core.PersistentVolumeBlock ||
			!slices.Equal(pv.Spec.AccessModes, pvc.Spec.AccessModes) || capacity.Cmp(*resource.NewQuantity(pin.Bytes, resource.BinarySI)) != 0 ||
			claimCapacity.Cmp(capacity) != 0 || !backingAffinity(pv.Spec.NodeAffinity, node) {
			return out, backingIdentityError()
		}
		if pin == j.Origin {
			mode, _ := azureutils.ParseDiskParametersForKey(pv.Spec.CSI.VolumeAttributes, azureconstants.AttachModeField)
			shares, err := azureutils.GetMaxShares(pv.Spec.CSI.VolumeAttributes)
			if err != nil || shares != 1 || mode != "" && !strings.EqualFold(mode, azureconstants.AttachModeControllerDriven) {
				return out, backingIdentityError()
			}
		}
		revisions = append(revisions, sc.ResourceVersion, pvc.ResourceVersion, pv.ResourceVersion)
	}
	if j.Origin.PVName == j.Cache.PVName || j.Origin.Handle == j.Cache.Handle {
		return out, backingIdentityError()
	}
	out.RequestID, out.VMID = request, strings.TrimPrefix(node.Spec.ProviderID, "azure://")
	data, _ := json.Marshal(revisions)
	out.Revision = blockRequestDigest(string(data))
	return out, nil
}

func backingTopology(terms []core.TopologySelectorTerm, node *core.Node) bool {
	if len(terms) == 0 {
		return true
	}
	for _, term := range terms {
		match := len(term.MatchLabelExpressions) > 0
		for _, expr := range term.MatchLabelExpressions {
			match = match && slices.Contains(expr.Values, node.Labels[expr.Key])
		}
		if match {
			return true
		}
	}
	return false
}

func backingAffinity(affinity *core.VolumeNodeAffinity, node *core.Node) bool {
	if affinity == nil || affinity.Required == nil {
		return false
	}
	for _, term := range affinity.Required.NodeSelectorTerms {
		match := len(term.MatchExpressions)+len(term.MatchFields) > 0
		for _, expr := range term.MatchExpressions {
			match = match && expr.Operator == core.NodeSelectorOpIn && slices.Contains(expr.Values, node.Labels[expr.Key])
		}
		for _, expr := range term.MatchFields {
			match = match && expr.Key == "metadata.name" && expr.Operator == core.NodeSelectorOpIn && slices.Contains(expr.Values, node.Name)
		}
		if match {
			return true
		}
	}
	return false
}
