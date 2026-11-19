# Azure Disk origin boundary

This package has a raw pre-attachment reservation adapter and a separate
owner-admission boundary. It is not a DeviceProvider service. `azuredisk.DriverOptions.Origin` is
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

**v1 mapping:** after authenticating the caller and validating the exact
provider, a future endpoint can map `Capacity` to both capacity fields (one
slot) and `Reserve`/`Lookup` to RESERVED snapshots of the binding, with the
configured pool and actual byte count. These Go methods are not wire-compatible
RPCs. They must not be used to serve lease snapshots once ownership is enabled:
first unify the journal with active/held state and the provider-wide request
ledger for *all* mutating methods.

## What is not integrated

* **Secure endpoint and ownership lifecycle:** `pkg/azuredisk/azuredisk.go`
  registers CSI services only. Before exposing any DeviceProvider endpoint,
  integrate the canonical v1 bindings and authenticate and authorize every
  operation, including retries. Acquisition, renewal, validated cleanup,
  release, owner-clock/high-water persistence and cross-node physical fencing
  have no production implementation. `Admit` is not a wire-level Acquire handler.
* **Provider exclusion and identity:** `Authority.Begin` must cover *all*
  controllers, nodes and out-of-band actors. `Session.ProveOrdinary` must bind
  the actual effect targets, not just trust a CSI handle, mount path, LUN or PV.
  Async ARM/QAD work and uncertain completion need durable in-flight tracking
  before submission and reconciliation before enrollment/reuse, even when an
  RPC returns success. `End` releases admission; it does not mark a disk safe.
* **Physical proof:** `Session.ProveExclusive` needs an independent fence
  against other writers; an ARM GET is corroboration, not that fence.
  `Session.IdentifyOpened` needs a trusted, platform-specific exclusive-open
  and attestation path binding the device to this allocation, node and epoch.
  No such resolver ships here. There is no cleanup-receipt issuer, previous
  writer IO revocation, release or successor path. Retain ownership until
  these can be proved; do not turn unstage, TTL or a force-detach into proof.

Tests use local Go doubles. Their serialized authority models the contract; it
does not demonstrate distributed fencing or authorize deployment.
