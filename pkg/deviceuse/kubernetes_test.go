// Copyright 2026 The Kubernetes Authors.
// Licensed under the Apache License, Version 2.0.

package deviceuse

import (
	"context"
	"testing"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"

	pb "sigs.k8s.io/azuredisk-csi-driver/pkg/deviceuse/usev1"
)

func sourceFixture(t *testing.T) (KubernetesResolver, *fake.Clientset, *pb.UseIdentity) {
	t.Helper()
	d, _, _, id := fixture(t)
	kube := fake.NewSimpleClientset(
		&core.Node{ObjectMeta: metav1.ObjectMeta{Name: d.Scope.Node, UID: "node-uid", ResourceVersion: "1"}},
		&core.Namespace{ObjectMeta: metav1.ObjectMeta{Name: d.Scope.Namespace, UID: "ns-uid", ResourceVersion: "1"}},
		&core.PersistentVolume{ObjectMeta: metav1.ObjectMeta{Name: "pv", UID: "pv-uid", ResourceVersion: "1"},
			Spec: core.PersistentVolumeSpec{VolumeMode: ptr.To(core.PersistentVolumeBlock), AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce},
				ClaimRef: &core.ObjectReference{Name: "claim", Namespace: "sources", UID: "pvc-uid"},
				PersistentVolumeSource: core.PersistentVolumeSource{CSI: &core.CSIPersistentVolumeSource{
					Driver: d.Scope.Driver, VolumeHandle: "disk"}}},
			Status: core.PersistentVolumeStatus{Phase: core.VolumeBound}},
		&core.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "claim", Namespace: "sources", UID: "pvc-uid", ResourceVersion: "1"},
			Spec:   core.PersistentVolumeClaimSpec{VolumeMode: ptr.To(core.PersistentVolumeBlock), VolumeName: "pv", AccessModes: []core.PersistentVolumeAccessMode{core.ReadWriteOnce}},
			Status: core.PersistentVolumeClaimStatus{Phase: core.ClaimBound}},
	)
	return KubernetesResolver{Scope: d.Scope, Client: kube, Native: func(context.Context, *core.PersistentVolume, *core.Node) (Resource, error) {
		return Resource{Key: "independent-native-uuid", Target: "disk"}, nil
	}}, kube, id
}

func TestKubernetesSourceRejectsReplacedOrForeignPins(t *testing.T) {
	for _, kind := range []string{"valid", "pv-uid", "driver", "handle", "claim-namespace", "pvc-uid", "namespace-uid", "node-uid", "placement"} {
		t.Run(kind, func(t *testing.T) {
			r, kube, id := sourceFixture(t)
			ctx := t.Context()
			pv, _ := kube.CoreV1().PersistentVolumes().Get(ctx, "pv", metav1.GetOptions{})
			switch kind {
			case "pv-uid":
				pv.UID = "replacement"
			case "driver":
				pv.Spec.CSI.Driver = "foreign.example"
			case "handle":
				pv.Spec.CSI.VolumeHandle = "another-disk"
			case "claim-namespace":
				pv.Spec.ClaimRef.Namespace = "foreign"
			case "pvc-uid":
				pvc, _ := kube.CoreV1().PersistentVolumeClaims("sources").Get(ctx, "claim", metav1.GetOptions{})
				pvc.UID = "replacement"
				_, _ = kube.CoreV1().PersistentVolumeClaims("sources").Update(ctx, pvc, metav1.UpdateOptions{})
			case "namespace-uid":
				ns, _ := kube.CoreV1().Namespaces().Get(ctx, "sources", metav1.GetOptions{})
				ns.UID = "replacement"
				_, _ = kube.CoreV1().Namespaces().Update(ctx, ns, metav1.UpdateOptions{})
			case "node-uid":
				n, _ := kube.CoreV1().Nodes().Get(ctx, "node", metav1.GetOptions{})
				n.UID = "replacement"
				_, _ = kube.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{})
			case "placement":
				pv.Spec.NodeAffinity = &core.VolumeNodeAffinity{Required: &core.NodeSelector{NodeSelectorTerms: []core.NodeSelectorTerm{
					{MatchFields: []core.NodeSelectorRequirement{{Key: "metadata.name", Operator: core.NodeSelectorOpIn, Values: []string{"other-node"}}}},
				}}}
			}
			_, _ = kube.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
			called := false
			r.Native = func(context.Context, *core.PersistentVolume, *core.Node) (Resource, error) {
				called = true
				return Resource{Key: "actual-native-uuid", Target: "disk"}, nil
			}
			_, err := r.Resolve(ctx, id)
			if (kind == "valid") != (err == nil) || called != (kind == "valid") {
				t.Fatal(kind, called, err)
			}
		})
	}
}

func TestKubernetesSourceRechecksUIDAfterNativeObservation(t *testing.T) {
	r, kube, id := sourceFixture(t)
	r.Native = func(ctx context.Context, pv *core.PersistentVolume, _ *core.Node) (Resource, error) {
		pv.UID = "replacement"
		_, err := kube.CoreV1().PersistentVolumes().Update(ctx, pv, metav1.UpdateOptions{})
		return Resource{Key: "native-uuid", Target: "disk"}, err
	}
	if _, err := r.Resolve(t.Context(), id); err == nil {
		t.Fatal("PV replacement during independent observation granted a use")
	}
}
