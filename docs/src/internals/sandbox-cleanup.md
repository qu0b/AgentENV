# Sandbox stop confirmation and durable cleanup

A captured snapshot does not prove the source runtime stopped. A missing
in-memory backend handle after server restart does not prove physical absence.
The orchestrator records these facts separately so a failed stop cannot make
occupied capacity available or authorize a second runtime.

## Pause and failed launch

Pause captures and persists the original snapshot, then stops its backend.
Only a successful stop followed by a synchronous update of `runtime_stopped`
allows pause to return success and publish its lifecycle event. If either step
fails, the original handle and snapshot remain available for retry. Capacity
continues to count that sandbox until stop confirmation succeeds. Retrying
pause or resume first retries that same backend; it does not capture another
snapshot or launch another VM while the stop is unconfirmed.

A failed start or readiness wait also attempts stop. A failed create whose
runtime cannot stop becomes `cleanup_pending`. A failed resume retains its
paused snapshot with `runtime_stopped=false`; a later resume must finish the
original attempt's stop before building another backend. Proxy-target validation
finishes before the launch publishes `Running`. Each successful new launch
clears the prior stop proof.

These guarantees depend on the backend's `stop()` contract. Tests using a mock
backend establish orchestrator ordering, not that Firecracker, ublk, network
namespaces, mounts, and volumes have physically converged on a target host.

## Delete and restart

Before deleting a runtime or artifacts, the persister synchronously writes a
`deleting` record under the original sandbox ID in `records.db`. Public inventory
exposes incomplete deletion as `cleanup_pending`; connect/resume cannot reopen
it. DELETE returns success only after stop confirmation, applicable volume
cleanup, artifact removal, and synchronous deletion of the record. Artifact
removal precedes record removal, so a filesystem failure retains durable debt.
Retries use the same ID and original metadata. Runtime handles whose stop fails
remain available in the current process without a proxy route.

On restart, `deleting` records reload as `cleanup_pending`. If a prior stop was
durably confirmed, deletion can finish without a live handle. Without that
proof, deletion returns an error and retains the record: native reconciliation
is required. A record left `resuming` always reloads with unconfirmed cleanup,
even if its metadata contains the older source runtime's stop proof. Its
record, snapshot artifacts, and existing paused-image protections are retained.
It is not automatically resumed or discarded.

No general host reconciliation command is implemented here. Do not infer stop
from inventory absence, manually flip the proof bit, or delete these records
as cache. Recovery must identify and stop the original runtime and resolve its
owned resources before recording completion. Node loss, partial storage loss,
and cross-node failover still require qualification and further implementation.

## Cutover and rollback

This fork is based on AgentENV 0.2.0. Qualification of an earlier native 0.1.3
stack does not qualify this revision. Drain old listeners and reconcile their
runtimes before switching binaries. Back up the private state consistently with
its original node identity; do not clone it across active nodes.

The new `runtime_stopped` field defaults to false for legacy records. Old paused
records therefore cannot resume on a fresh process merely because they contain
a snapshot. Drain or explicitly reconcile those records before the cutover.
The record envelope remains version 1, but its lifecycle contract has changed:
older binaries may discard interrupted resumes or unknown deleting records.
Rollback must use a binary that preserves this contract, or a separately
verified backport, and must retain the records and artifacts. Do not start an
older persister on the new state directory.

The runner must accept `cleanup_pending` inventory for ownership-checked DELETE
retries while retaining allocation settlement, reservation, and metadata fences.
This does not enable approval parking or durable tool execution by itself.

## Evidence

The lifecycle tests inject failures in backend stop, pause confirmation,
tombstone persistence, and durable record/artifact deletion. Failed create and
resume tests cover both start and readiness failures and assert retention of
the identical backend handle across retries. API tests exercise authenticated
inventory, filtering, refused connect, and confirmed versus unconfirmed DELETE.

The process test uses real RocksDB persistence and a real filesystem permission
failure. It kills the child with SIGKILL after DELETE has failed, opens the same
state in a fresh orchestrator, verifies retained evidence, repairs only the test
directory permissions, and retries deletion. Its VM is mocked. Native KVM/ublk,
volume, broker, and approval restart acceptance remain separate required gates.

The API's create-failure/finalization paths still release pending volume owners
and may delete restored volumes after an orchestrator error without requiring
confirmed runtime deletion (`src/api/impls/sandbox.rs`). They need their own
durable ownership correction and failure tests before mounted-volume recovery
can be qualified. The orchestrator tests here do not cover that API boundary.
