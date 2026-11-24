# CompositeDisk source-use gates

The opt-in `compositedisk.provider.v1.DeviceUse` service has exactly three RPCs:
`AcquireUse`, `GetUse`, and `ReleaseUse`. It does not reserve capacity, provision
a disk, attach it, format it, return a device path, or own file descriptors.
Normal CSI ControllerPublishVolume and Block NodeStageVolume/NodePublishVolume
remain responsible for attachment and private node publication.

Each request carries the exact caller-persisted printable ASCII `use_id`
(1-128 bytes), full composite CSI handle including CR UID, node ID, and
`SourceVolume {driver, volume_handle, pv_name, pv_uid}`. An ACTIVE response is
a durable teardown hold, **not device or filesystem readiness**.

## Lifetime and native serialization

- Fresh acquisition independently verifies the existing Bound Block/RWO
  PV/PVC binding, configured namespace/node UIDs, CSI identity, node placement,
  and real exclusive Azure disk/VM identity. No creation provenance is required.
- A different live use of the same actual disk GUID is rejected, including a
  different PV that aliases it. An exact ACTIVE retry revalidates the source.
  Missing/replaced metadata fails acquisition but never erases a retained hold.
- GetUse reads the exact durable identity/state. Missing records return
  NotFound; that is never cleanup evidence or permission to tear down a source.
- ReleaseUse trusts the authenticated, independently authorized owning node
  to have completed actual composite/application/mapper cleanup and closed its
  FDs. Exact existing-owner release can use retained identity after Kubernetes
  source metadata disappears. Metadata absence itself never releases anything.
  A never-seen ID is permanently recorded RELEASED only after live scope
  validation; early release is not a success-without-record shortcut.
- RELEASED is permanent. Replaying an old release cannot clear a successor's
  active hold. A later activation needs a new, persisted use ID.

All relevant native mutations admit a durable operation token before effects.
Proven pure request validation runs before admission. Errors after native
handler entry remain ambiguous regardless of their gRPC status code.
Teardown, modification, destruction, and ambiguous VM disk-name aliases are
blocked while the source is ACTIVE. Existing-source **Block** attach/stage/
publish remains allowed on its owning node, without adding a use token to CSI.
Preparation also retains operation admission so detach recovery cannot race
acquisition. Successful completion is persisted before returning the CSI reply,
even when the caller has disconnected. Only exact completion history reconciles
a lost journal-write reply. Native errors/unknown effects retain admission
across process reconstruction; neither ReleaseUse nor stale completion clears
another operation's marker.

Release the use **after composite cleanup but before native child unpublish/
unstage/detach**. Otherwise the source gate correctly blocks the caller's own
teardown. There is no timeout, renewal, node-loss unlock, automatic recovery,
or reclaim on disconnect. Authoritative recovery of unknown effects is future
work; never erase a journal to make a source available.

## Explicit configuration

Default behavior is unchanged: no custom listener or use gate without
`--device-use-config=/protected/device-use.json`.

```json
{
  "scope": {
    "Driver": "disk.compositedisk.infinitydb.io",
    "Namespace": "applications", "NamespaceUID": "<namespace UID>",
    "Node": "worker-a", "NodeUID": "<node UID>"
  },
  "journalNamespace": "applications",
  "journalName": "azure-device-uses",
  "journalUID": "<actual ConfigMap UID>",
  "endpoint": ":9710",
  "serverCertFile": "/protected/tls.crt",
  "serverKeyFile": "/protected/tls.key",
  "clientCAFile": "/protected/client-ca.crt",
  "grants": [{
    "uri": "spiffe://infinity/compositedisk/node/worker-a",
    "node": "worker-a", "namespace": "applications",
    "pvNames": ["existing-origin-pv"]
  }]
}
```

The scope driver must equal the process `--drivername`, PV CSI driver and
StorageClass provisioner. Official `disk.csi.azure.com` is not renamed. Use
separately controlled canary identity/registration/plugin paths when qualifying
this fork; do not replace a managed addon as part of this source change.
QAD, KataMount, migration monitoring and taint removal must be disabled for the
opt-in domain, so every relevant native mutation traverses the same gate.

Provision the ConfigMap once, offline, using
`deviceuse.InitialData(scope)`. Pin its API-server UID. `device-use.v1` is the
sole data key and contains the canonical version-1 snapshot. Runtime never
initializes missing state, drops tombstones, migrates old state or recreates the
ConfigMap. All controller/node replicas that can mutate these sources must
share this exact journal and scope. No second journal/driver or unguarded actor
may independently manage the same physical source. This is native provider
serialization, not a fence against privileged out-of-band mutations.
Native CSI carries no use token, so the operator must exclude competing source
consumers rather than treating an ACTIVE hold as an ordinary-open security fence.
The prototype retains at most 4096 use records and 768 KiB of use history;
native completion/cleanup has separate reserved journal headroom. Exhaustion
fails new uses closed, never prunes tombstones or releases an existing use.

TLS 1.3 mutual authentication requires an independently configured exact
certificate URI, node, namespace, and PV-name grant. `readOnly:true` permits
GetUse only. There are no controller/pool grants or caller-supplied permissions.
PV names are explicitly authorized: a newly provisioned template PV needs an
operator grant/configuration refresh before acquisition. There is no implicit
namespace wildcard or automatic permission bootstrap.
Certificate expiry does not release a hold. Restrict journal write permission
to the provider and source reads to the configured scope; the use service never
writes source PV/PVC metadata or deletes source resources.

This breaks the old private DeviceProvider protocol and journal layout.
Do not mix binaries or point an empty new journal at unresolved old holds.
The existing ZIPs under `experiments/` remain historical, uncompiled artifacts.
The canonical protobuf is mirrored in `pkg/deviceuse/usev1/device_use.proto`;
regenerate with `sh pkg/deviceuse/usev1/generate.sh` using protoc 3.12.4,
protoc-gen-go 1.36.12 and protoc-gen-go-grpc 1.5.1.

Unit/race tests use fake Azure/Kubernetes state and local authenticated gRPC,
not live disks or deployment. They do not qualify physical storage readiness.
