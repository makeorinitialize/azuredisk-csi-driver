# Azure Disk provider holds

## Active mode: externally owned source PVCs

The composite references **existing** origin/cache PVCs. It owns neither their
physical provisioning nor their Kubernetes lifetimes. The origin must already
contain a compatible ext4 filesystem; the cache is an existing raw block
source. This provider does not format either source, create a backing PVC, or
delete a source PVC/PV/VolumeAttachment/disk when a composite is deleted.

DeviceProvider v1 adds `Lease.hold_policy` at field 11 without renumbering
existing fields. All external-source leases, including RESERVED, advertise
`HOLD_POLICY_EXPLICIT_RELEASE` and a zero `renew_by_unix_seconds`. `Reserve` resolves exact
external identities and establishes a durable use authorization; it is not a
disk creation request. The source must have explicit `maxShares=1`, stable disk
GUID/size and consistent independent Azure disk/VM observations. An existing
imported or copied disk is not rejected merely for its creation history.

`--online-block-domain-config=/protected/online.json` enables the external
source domain on controller and node processes. Use a **separate canary CSI
name**, never the managed AKS addon. The configured effective `originDriver`
must exactly equal `--drivername`, the StorageClass provisioner and PV CSI
driver. See [the isolated deployment recipe](../../docs/online-block-canary.md).

### Exact external source journal

The source is the cluster-scoped `storage.infinitydb.io/v1alpha1`
CompositeVolume selected by its exact `composite_volume_id` (CR name).
Its annotation `dmstack.csi.infinitydb.io/external-source-pvcs-v1` has canonical
field order:

```json
{
  "version": 1,
  "owner": "<CR-name>.<CR-UID>",
  "config": {
    "namespace": "sources",
    "namespaceUID": "<namespace-UID>",
    "nodeName": "<node-name>",
    "nodeUID": "<node-UID>",
    "originDriver": "disk.dmstack.infinitydb.io",
    "cacheDriver": "localdisk.dmstack.infinitydb.io"
  },
  "outer": {"name": "application", "uid": "<outer-PVC-UID>"},
  "origin": {
    "name": "my-origin", "uid": "<PVC-UID>",
    "pvName": "<PV-name>", "pvUID": "<PV-UID>",
    "className": "<SC-name>", "classUID": "<SC-UID>",
    "driver": "disk.dmstack.infinitydb.io",
    "handle": "<exact-Azure-ARM-ID>", "bytes": 1073741824
  },
  "cache": {
    "name": "my-cache", "uid": "<PVC-UID>",
    "pvName": "<PV-name>", "pvUID": "<PV-UID>",
    "className": "<SC-name>", "classUID": "<SC-UID>",
    "driver": "localdisk.dmstack.infinitydb.io",
    "handle": "<exact-cache-handle>", "bytes": 1073741824
  }
}
```

`owner` is the composite's UID-bearing handle, never the outer PVC's UID.
The actual outer filesystem/RWO PVC must contain exact same-namespace names
in `dmstack.csi.infinitydb.io/origin-pvc` and `/cache-pvc`. Self references,
same-source references, slash/ordinal expansion, UID drift and driver aliases
are rejected. Sources must be Bound RWO Block PVC/PVs, with exact claimRef,
class, driver, handle and requested/status/PV capacity agreement and compatible
pinned placement. Source ownership labels, source finalizers, deterministic
names, native-create tags and Empty-disk provenance are **not prerequisites**.

Optional driver names default to the official names when absent, preserving
old omitted bytes. Names are exact lowercase DNS subdomains, at most 63
characters, distinct, with cross-role official aliases rejected. The provider
config's old class/byte fields are optional administrator restrictions compared
against the role pins; they are not part of this source journal's config.

The old `backing-pvcs-v1` journal fails closed and is never rewritten into the
new format. An initial journal with empty `owner` cannot admit a fresh hold.

### Lifetime and native admission

Bootstrap the provider's `external-domain.v1` ConfigMap once, offline, using
`InitialOnlineDomain(identity)`, and pin its API-server UID. Every instance
serving this canary name must install the same durable domain **before traffic**.
Missing or incompatible records never mean "ordinary and safe". Do not reset,
replace, restore or reuse an old journal; exclude unguarded replicas and
out-of-band mutations. The runtime writes only its journal, never source
metadata or finalizers.

The record under `external-domain.v1` now requires internal `Version: 2`.
Selected block journals require `hold-policy.v1=explicit-release` and
`deadline=0` in every phase. Old domain versions, missing/unknown policy,
nonzero deadlines, renewal history, and old renewing/expired phases are
quarantined without rewriting, releasing, detaching or reusing the source.
Even an old selected journal with no owner is not silently upgraded. There is
no automatic migration; do not reset retained old state to bypass this check.
Coordinate this protocol change with the facade and cache provider before use;
the previous deadline-requiring facade is incompatible.

All relevant native CSI mutations cross a resourceVersion-CAS Begin/End
barrier, including work admitted before registration. Unconfirmed effects or
completion retain busy state, without TTL recovery. Ordinary unrelated native requests
remain admitted; this prototype serializes native operations. A held source
cannot be modified/deleted or collide with native create/clone targets.
No-force protection remains for the selected disk, including mixed batches.

After a successful native handler, only an actual Kubernetes resourceVersion
conflict retries completion CAS. Each attempt rereads the full current object,
checks the exact operation token/method/input/target and preserves concurrent
acquisition/release history; the handler is never rerun by this retry. Completion
records the native result digest alongside clearing Busy. A lost/unknown reply
is reconciled by reading that exact completion receipt, not by repeating the
write or physical effect. A historical match acknowledges only that operation,
never clears a successor. Only the latest receipt is retained (bounded state);
if history has been superseded or cannot be proven, return uncertainty instead
of assuming success. Generic ABORTED/transport errors are not CAS conflicts.

`Reserve` returns RESERVED before attachment. A properly attached source on
the exact pinned VM may also be resolved, but that is not opened-FD evidence.
An unattached source must undergo the **normal guarded ControllerPublish**
to the selected node before `Acquire`; reservation must not retire it during
that expected attachment. The composite controller coordinates that native
attachment; a Bound PVC by itself is not a stage or device grant.

The provider journal retains the complete external snapshot and immutable
allocation/disk/VM/owner identity. Existing-owner lookup, replay and
cleanup do not depend on source API objects surviving. Fresh admission still
requires all live exact source pins. Retained-owner physical observations read
the pinned Azure resource IDs directly; absence, replacement or uncertainty
never releases a hold. No new source finalizers are required by this adapter.

Holds do not expire and require no renewal or periodic supervisor. Elapsed time,
disconnects and process reconstruction cannot revoke the owner or admit a
successor. `Renew` is deprecated and returns FAILED_PRECONDITION for an exact
explicit-release owner, without changing state. Authentication, exact node/epoch
checks and fresh independent attachment/custody evidence remain mandatory;
certificate validity is not a lease timer. Unknown effects remain retained.

### Node-use release is not allocation closure

NODE Release requires independent exact physical teardown proof. For non-Kata
that is native-host cleanup; for paired Kata it requires exact guest cleanup
**and** runtime hot-unplug. These receipt types are not interchangeable. Neither
TTL, source absence, client assertions nor an absent mount proves cessation of
IO. The real receipt authority must resolve the opaque receipt independently.

After verified node-use release, ordinary node/controller unpublish may detach
the source **without** closing the allocation. Pre-acquire native unpublish may
return an unacquired reservation to the selected/unattached state.
ALLOCATION Release is a separate irreversible authorization tombstone for
composite deletion, allowed only with no acquisition or a verified released
owner at its retained epoch. It does not dispatch source destruction.
GetLease returns NOT_FOUND after closure, while exact closure retries acknowledge
history. The external source remains under its ordinary native CSI/user owner.

**Current limitation:** this is still a first-owner, single-allocation adapter.
A higher epoch after verified node release is explicitly unsupported; neither
StatefulSet restart nor reusable composite activation is claimed. Final closure
does not delete or permanently retire the backing PVC, but that old composite
authorization cannot be reopened.

### Opened-FD identity

Validate is read-only and requires independent current custody. Under durable
exclusive provider ownership and operation exclusion, perform a transient
exclusive-unused probe, open and independently verify the retained
`O_RDWR|O_CLOEXEC` original **while the probe is still held**, then close only
the probe. Keep that original OFD unchanged through transfer, validation,
formatter/DM claims and cleanup. `O_EXCL` is not a security fence against all
ordinary opens and must not be held for the lifetime of the registered device.

`LinuxBlockOpenAuthority` checks the same current/retained OFD using
`F_DUPFD_QUERY`, block mode/dev_t and independent identity/size evidence.
Unsupported kernels and unknown identity fail closed. Never accept caller paths,
echoed GUIDs, dev_t alone, or a close/reopen as equivalent custody.

No first-format service is active. The formatter/provenance experiments are
preserved in the inactive archive, not used as runtime prerequisites. The
current online server still fails NODE Release/Validate closed unless a real
node custody/teardown authority is installed; source resolution alone is not
physical readiness.

## Static ordinary holds and legacy APIs

`InitialBlockHold`, `NewBlockHold`, `WithOpenedAuthority`,
`NewPublishedBlockServer` and the ordinary setup/teardown guards remain available
for explicitly pre-enrolled static selections, now with the same explicit-release
policy marker and zero deadline. Preserve journal UID and all owner/release
history. Old timed static journals fail closed, not migrated or released.

The legacy raw-reservation and `Origin` APIs remain separate programmatic
experiments, off by default. `NewReservations` requires pinned inventory,
exclusive provisioning scope and the original CAS journal; it must not be
used to create or own externally declared sources. Legacy wire defaults are
GetCapacity/Reserve/GetLease, with optional guarded controller acquisition;
no implicit dynamic adoption, force takeover, release or source reuse exists.

The superseded automatic backing-PVC creation/provenance experiment (including
its detailed docs/tests) is preserved in
`experiments/automatic-backing-pvc-before-external-sources.zip`, with per-file
SHA-256 manifest. It is not compiled or referenced by the external-source path.

The superseded timed block renewal implementation/tests are preserved in
`experiments/timed-block-renewal-before-explicit-release.zip`, with their source
commit and per-file SHA-256 manifest. They are not compiled. Raw-reservation
and `Origin` experiments retain their separate legacy timed semantics; an
unspecified wire policy there is never explicit-release authority.

Tests use local authenticated gRPC and fake Kubernetes/Azure state. Real guest
kernel/formatter/DM qualification establishes mechanism compatibility under a
gate model, not production Azure identity or cleanup authority.
