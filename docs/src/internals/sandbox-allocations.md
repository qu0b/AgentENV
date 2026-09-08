# Durable sandbox allocation requests

A lost `POST /sandboxes` response does not prove that creation failed. The
orchestrator continues its lifecycle task after the caller disconnects, and an
empty inventory may be observed before a delayed request even arrives. Retrying
an ordinary create can therefore allocate another guest.

Clients which need durable allocation ownership use
`POST /sandbox-allocations/{allocationID}` with the ordinary `NewSandbox` body.
The key is a non-nil UUID generated and retained by the client before sending.
First read `GET /sandbox-allocation-owner` and retain its `ownerId` in the original
client intent. Send that value as `X-Agentenv-Allocation-Owner` on every keyed
POST, GET, and DELETE. Missing or ambiguous owner headers are rejected; a different
owner returns 409 before claiming, reading, or fencing the allocation. A URL or
DNS change must not let another node's empty inventory settle the original work.
This dedicated endpoint is intentional: an older server returns an error instead
of silently ignoring an idempotency header and performing an untracked create.
`POST /sandboxes` and cold creation retain their existing contracts; metadata
named `allocationId` alone does not opt a request into this protocol.

## Request ownership

The node commits an immutable owner ID, allocation ID, server-assigned sandbox ID, SHA-256
request digest, and pending state before template resolution, volume restoration,
volume reservation, or VM launch. The assigned ID is passed through the actual
orchestrator launch. The digest covers the serialized typed request recursively,
including environment, network, security, metadata, timeout, and volume options;
object key order does not matter. The journal does not retain raw request bodies
or credentials.

The complete keyed handler runs in an owned task, including volume finalization
and publication of its settlement receipt. Losing the HTTP caller does not drop
that task. A duplicate key returns 409 and never invokes create work again,
including after an error or restart. A different body for the same key also
returns 409. Replays do not reconstruct an old 201 response or its access tokens.

`GET /sandbox-allocations/{allocationID}` reads the original receipt. All three
methods use the normal node API key authentication. These are node-local records,
not tenant authorization grants or distributed scheduling assignments. The
owner endpoint requires the same API key and creates no VM. Requests
must reach the original durable state owner. A 404 only says that this owner has
no receipt; it does not fence an arriving POST.

## Cancellation and receipt interpretation

`DELETE /sandbox-allocations/{allocationID}` durably fences the key. It never
deletes the record or guest. If cancellation wins before a create claim, it
commits a tombstone with no sandbox ID; all later creates for that key are refused.
If creation already owns the key, cancellation records `cancelRequested=true`
and allows the original operation to settle. A client must not race guest deletion
against a pending create handler.

| State | Meaning |
| --- | --- |
| `pending` | The original handler has not published settlement. It may still create or finalize the assigned guest. |
| `settled` | The handler returned and cannot start more create work. This includes returned HTTP errors. It does **not** prove that the guest exists or was deleted, or that all failed-launch resources were released. |
| `cancelled` | Cancellation won before any create claim. The sandbox ID and request digest are null. |
| `interrupted` | The callback panicked, or an assigned record belongs to an earlier server incarnation. Host reconciliation is required. |

The client retains its original nonsecret ownership metadata and verifies it
against the assigned guest before destructive cleanup. A settlement receipt,
terminal application result, or an empty new process's in-memory inventory is
not proof of physical VM deletion. In particular, this change does not implement
post-SIGKILL Firecracker/ublk recovery or cleanup of interrupted volume restoration.
It exposes that uncertainty without ever authorizing another allocation.

## Persistence and cutover

The durable owner UUID, format version, and allocation records live in
`allocations.db` under `orchestrator.persisted_sandbox_store_path`,
separate from paused-sandbox `records.db` and artifacts. Writes use the shared
`LocalKvStore` with `LocalStoreDurability::Sync`. A short process-local mutation
lock is retained through the blocking database write even if the caller drops;
it is not held during VM or network work. RocksDB excludes a second database
owner. This is not a multi-node consensus or failover protocol.

Preserve this database, cancellation tombstones, the node's state identity, and
the original client allocation intents through releases and backups. There is
no online deletion/TTL for these records: deleting one can turn a delayed replay
into a new allocation. Disk growth is proportional to distinct keys and must be
included in capacity planning. A future reclamation protocol must first fence
all old senders and establish that their keys can never be replayed.

Route keyed clients only to servers implementing this endpoint. Older servers
cannot reconcile these receipts and must not own keyed traffic during rollback.
Recreating a database gives it a different owner ID; old intents must fail rather
than adopting that new owner. Missing or conflicting identity in an existing
record is an error. Do not copy live RocksDB directories, route one key across independent nodes, or
reinterpret historical unkeyed requests as keyed work. Existing unkeyed guests
and unresolved legacy creates still require their original cleanup policy.

## Verification

Focused Rust tests cover durable cancellation before a delayed POST, concurrent
duplicates, changed payloads, caller cancellation while the handler runs,
independent allocations during a blocked operation, callback panic, corruption,
future format refusal, and real child-process SIGKILL followed by reopen. Router
tests use actual API authentication, generated request/response handling, and a
snapshot test double, including wrong-node rejection and ambiguous owner headers.
An orchestrator test verifies that the receipt's sandbox ID
is the ID used by the backend, metadata store, and deletion path.

These tests do not allocate a native VM or establish a client integration,
distributed recovery, power-loss durability, or residency guarantee.
