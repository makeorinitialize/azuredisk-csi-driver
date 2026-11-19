# Azure Disk origin boundary

This package has a pre-attachment reservation service and a separate
owner-admission boundary. `azuredisk.DriverOptions.Origin` is
nil by default. There is deliberately no command-line switch, listener,
manifest change, or production `Authority`. Do not mix guarded and unguarded
driver instances for a provider domain containing origins.

`Origin.BeginCSI` denies ordinary CSI access to allocated origins, including
reserved and liveness-expired disks. For ordinary disks it holds provider
admission until the actual CSI method returns. Controller-driven and QAD disks
still use their original paths. `Origin.Admit` only admits a first owner with
independently proven exclusive access, or rechecks the exact live incarnation.
`WithOpenedDisk` requires independent attestation of the same handle used by
the operation. None of these methods releases, detaches, deletes, or takes over
an origin.

## Ordinary CSI-provisioned disposable Block PV (one use)

This mode uses neither raw reservation nor the origin attach adapter. Provision
the normal CSI volume first. Before its first publication, a trusted operator
must preselect the bound RWO Block PV and PVC (both names and UIDs), disk ARM ID,
Azure unique ID, capacity, VM ARM ID and incarnation, exact composite, node and
owner epoch. Seed a new ConfigMap with `InitialBlockHold(selection)`, pin its
server UID, and install the same `DriverOptions.PublishedBlock` on **all**
controller/node instances. Never reset, replace or roll back the journal.
Do not enroll over a running unguarded operation. Setup, release and rollback
protection assume exclusive RBAC, trusted CSI clients/target paths and no
out-of-band disk mutation. There is deliberately no deployment flag.

The actual controller publish and ordinary node setup run inside durable
setup markers. An ambiguous setup remains blocked, even across restart.
`Driver.PublishedBlockServer` uses the existing mTLS `DeviceProvider` service:
call `Acquire` with the exact enrolled Allocation, node, epoch and previous
epoch zero; use `GetLease` for observation and `Release` with mode NODE and
the authenticated cleanup receipt ID. `Acquire` only resolves an already
published attachment. It does not attach, open a raw device, or return a
device path. Both Azure RPs, exact disk GUID and explicit `maxShares=1` must
agree; PV metadata alone does not grant use. `Validate` is read-only and
requires independent node-side custody (below). `Renew` extends only the exact
still-live enrolled owner after fresh PV/PVC and both Azure RP observations;
it does not replace opened-device validation. No wire `Reserve` or ALLOCATION
release is implemented in this mode.
The one-minute liveness deadline is not a physical fence; expiration retains
the hold and continues blocking teardown.

Call `Renew` before the provider deadline with a fresh request ID (at most
1024 UTF-8 bytes); retry uncertain outcomes with the **same** ID and input.
Successful renewal input digests and deadlines commit together in the existing
ConfigMap CAS as a pending candidate. A confirmation CAS is authorized only
after observing that candidate while the **previous** deadline is still live.
Pending candidates grant no acquired lease; their new deadline cannot revive
an expired hold after a late/lost write response or restart. Retry a pending
candidate with its original request ID before the previous deadline, or
perform verified cleanup. Cleanup also accepts pending holds.
Exact confirmed replays recheck attachment and return the current live
owner snapshot without extending it, including after server restart. Reusing
an Acquire/Release key or changing a committed renewal input is rejected.
Expired holds cannot be renewed, and rejected claims do not consume history.
The one-use journal retains at most 2048 successful renewal IDs, never evicts
them, and returns RESOURCE_EXHAUSTED for new renewals when full; arrange
verified cleanup before that limit, not journal reset or slot reuse.
Legacy one-shot journals migrate on the first admitted renewal candidate without
discarding any old fields; legacy and upgraded journals both support verified
cleanup. Unsupported renewal history fails closed. Do not downgrade to an
older binary that does not understand renewal history.

Ordinary unpublish/unstage/delete admission and hold acquisition CAS the
same object. If teardown wins, hold admission is permanently closed **before**
the physical operation. If hold wins, teardown is denied until the exact
owner releases with independently verified runtime cleanup. Only then may
normal cleanup run (no force retry for the selection). Never reuse this slot.
Mixed detach batches exclude the selection from force retries regardless of
which disk's caller drains the batch. Controller publish/unpublish also reject
ambiguous same-basename handles; unrelated Delete/Modify/Expand operations
retain full ARM-ID targeting across resource groups.

`BlockCleanup.VerifyCleanup` is the integration point for a trusted receipt
resolver: verify completed hot-unplug and no further IO for the exact disk,
VM, composite, node and epoch. No receipt issuer, real sandbox attestation,
or Kata delivery mechanism ships here. IDs in the v1 allocation are opaque;
do not lowercase ARM IDs or interpret them as Kata tokens. The host must
independently bind its safe device token to the exact approved allocation.

**Physical Validate prerequisite:** install the hold returned by
`hold.WithOpenedAuthority(authority)` before constructing the published server.
The node's verified mTLS leaf, node, VM incarnation, composite, disk GUID,
allocation/generation and owner epoch must identify exactly one immutable
exclusive-open registration. The trusted node custodian must retain a
duplicate **at the original exclusive open**, independently obtain a
duplicate of the authenticated facade's **current** FD, and serialize against
its replacement, revocation and cleanup. The pins must stay valid through
the evidence check. Never look up a caller-supplied path or trust an FD
number, major:minor, receipt string or `ValidateRequest` as attestation.

`LinuxBlockOpenAuthority` is a same-kernel adapter for such custody. It uses
`F_DUPFD_QUERY` (unsupported kernels fail closed), verifies block mode and
reads `st_rdev` via `fstat`. Its `IdentifyOpened` hook has the existing
`Session.IdentifyOpened` contract: obtain the actual size from the handle
(e.g. BLKGETSIZE64) and independently correlate its SCSI/NVMe disk identity
with the enrolled Azure disk, generation and exclusive owner. Cross-node
deployments need an authenticated, fresh proof from that node, not an FD
number sent to a different kernel. No custody broker, device resolver or
attestation transport is installed by this patch. Without it, `Validate`
fails; successful validation is a snapshot, not permission for later IO.

## Raw reservations (one provider, one pool, one slot)

Bootstrap **once**, offline: enroll a dedicated, never-attached slot; create
one ConfigMap with `InitialJournal(pool)` as its data and pin the API-server
UID in protected provider configuration. Give the serving identity only get
and update on that object. Exclude all other writers, deletion, manual reset
and rollback/restore to an older journal. Never bootstrap over lost state or
assign another journal to this provider, resource ID or disk GUID.

`NewReservations` takes the namespaced ConfigMap client, pinned name/UID,
Azure subscription client factory, and `ProvisioningScope`. The slot's ARM
ID, size, location and optional inventory GUID are immutable. Inventory must
be independently enrolled by GUID; mere existence at that ARM ID is a
conflict, not permission to adopt. There is no import, delete or reuse.

`reservation_journal.go` stores versioned Reserve inputs and slot state in
one API object. `reservation.go` uses resourceVersion CAS, not process locks.
Only the confirmed `free → submitted` CAS winner may dispatch creation, using
the driver's empty-disk source builder. A retry in `submitted` only reads ARM.
The operation checks provisioning state, resource ID, stable GUID, size and
exclusive slot provenance before committing `reserved`. A crash just before
PUT, an ambiguous dispatch with no observable result, or an unavailable
journal fails closed. Do not repair by clearing the journal or resending PUT.
Ledger growth is bounded; history is never evicted to make room.

**Provisioning gate:** the Azure disk SDK exposes CreateOrUpdate, not a
conditional create. Before enabling the adapter, the provider operator must
supply `ProvisioningScope` that establishes exclusive control of the slot
against all other principals, delayed operations and ordinary CSI throughout
its lifetime. For dynamic creation it must also confirm allocatable capacity.
A GET/404, request tag, unused-looking disk, ConfigMap CAS or local mutex
alone does not establish that authority. No implementation enabling it in a
deployed driver is provided.

## DeviceProvider v1 reservation endpoint

`NewReservationServer(reservations, serverKeyPair, clientRoots, grants)` returns
a registered gRPC server, not a listener. A trusted host must supply the
`Reservations` above **with a nonempty inventory GUID**, verified exclusive
`ProvisioningScope`, dedicated listener and shutdown lifecycle before calling
`Serve`. No CSI startup path calls it. There is no wire route to dynamic disk
creation, even for an authenticated controller. If the provider operator
cannot establish exclusivity, leave the listener off; do not substitute a
local mutex or an always-successful scope.

Use explicit client CA roots and TLS 1.3. Configure each `ReservationGrant`
with SHA-256 of the client leaf certificate **DER**, `ReservationReader` or
`ReservationController`, and the exact composite IDs it may access. Both
roles may read pool capacity and their volumes' leases; only a controller
may reserve. There is no wildcard, CN, header or cluster-wide default.
Restart with new grants and certificates to rotate or revoke access.

In the facade controller's `--controller-config`, set the `origin` field as
below (replace these illustrative values; retain its other required fields):

```json
{"origin":{"id":"az-origin-A","address":"origin.provider.example:443",
  "serverName":"origin.provider.example","ca":"/etc/origin/server-ca.pem",
  "cert":"/etc/origin/controller.pem","key":"/etc/origin/controller.key"}}
```

`id` is **exactly** `RawPool.Provider`; the trusted inventory row's
`originPool` must be **exactly** `RawPool.PoolID`, with the actual origin
allocation unit (`RawPool.Bytes` for this fixed slot). Grant the composite
IDs the facade actually sends. Use a server certificate for that `serverName`
and the matching client certificate/key. Do not configure this as a cache route.

The public bindings in `api/v1` use the canonical `dmstack.provider.v1`
schema with only the Go package option changed (canonical LF SHA-256:
`304365e871c08efe7aea330bca6a1c2c00c0757a1ad1434c7f2a52f1af4ca2f0`).
Run `sh pkg/devicelease/api/v1/generate.sh` with its exact tool versions on
PATH to reproduce them. Separate processes interoperate by wire name and
field number; do not link two generated copies of the schema in one binary.
By default the RPCs are `GetCapacity`, `Reserve` and `GetLease`.

### Optional controller attachment

Pass the adapter returned by `Driver.WithOriginController(reservations)` to
`NewReservationServer` on a guarded controller, and install a `ReservationNode`
certificate grant with exact `NodeID` and composite IDs. Only this role can
call `Acquire`. All ordinary CSI instances and external principals must still
respect enrollment. Do not mix the independent `Origin.Admit` test boundary
with this journal as a second ownership authority. Older binaries reject
journals with acquisition fields; do not reset the journal on downgrade.

Admission binds the first valid request, owner epoch, ARM VM ID and VM
incarnation in one CAS (`attachPrepared`). Rejected new claims do not consume
journal space; already-bound keys still require an exact replay. Prepared
means **not dispatched**: only the same claim may retry its disk and VM
preflight, and it cannot repin the VM. If Azure positively identifies a
healthy replacement with a different VM GUID while both views show no disk
attachment, a CAS on that exact prepared record retires the old key and
returns the slot to `reserved`. The key stays bound: exact retries are
ABORTED, changed inputs are ALREADY_EXISTS. A fresh authorized request must
pass normal preflight. An incomplete read or CAS conflict cannot clear the
intent, and retirement never applies after dispatch became possible.
Only a confirmed `attachPrepared` →
`attaching` CAS winner may call the existing normal `AttachDisk` implementation.
Azure's non-shared disk semantics exclude other VMs; there is no second
distributed fencer. The adapter does not enter the dangling-disk detach
branch. It verifies the current GUID, size, sharing configuration, disk
`Attached` state and `ManagedBy`, plus the exact managed-disk entry on a
freshly read, successfully provisioned VM/VMSS instance. A LUN alone, an
accepted operation or a Kubernetes attachment object cannot confirm access.
Once `attaching` is durable, even loss of that CAS reply or a crash before
the call leaves dispatch ambiguous. Only read-only reconciliation is allowed;
an unattached observation does not permit redispatch or takeover.

`Acquire`, `GetLease` and compatible `Reserve` replays return the current
owner snapshot. A one-minute deadline uses provider time (hosts need reliable
clocks); expiry latches HELD and never grants another epoch. **No raw device
is handed off.** No node path, major/minor, cleanup proof, release, renewal
or successor is issued. `Validate`, `Renew` and `Release` remain UNIMPLEMENTED.

## What is not integrated

* **Production host and ownership lifecycle:** `pkg/azuredisk/azuredisk.go`
  registers CSI services only. Production listener, grant provisioning,
  automatic renewal scheduling, validated cleanup and successor ownership
  remain pending. The published-block RPC implements renewal and receipt-gated
  node release, but installs neither a renewal loop nor a receipt authority.
* **Provider exclusion and identity:** `Authority.Begin` must cover *all*
  controllers, nodes and out-of-band actors. `Session.ProveOrdinary` must bind
  the actual effect targets, not just trust a CSI handle, mount path, LUN or PV.
  Async ARM/QAD work and uncertain completion need durable in-flight tracking
  before submission and reconciliation before enrollment/reuse, even when an
  RPC returns success. `End` releases admission; it does not mark a disk safe.
* **Local custody:** `Session.IdentifyOpened` needs a trusted, platform-specific exclusive-open
  and attestation path binding the device to this allocation, node and epoch.
  No such resolver ships here. There is no cleanup-receipt issuer, previous
  writer IO revocation, release or successor path. Retain ownership until
  these can be proved; do not turn unstage, TTL or a force-detach into proof.

Tests use local Go doubles. Their serialized authority models the contract; it
does not demonstrate distributed fencing or authorize deployment.
