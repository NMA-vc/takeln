# Design note: a `Running` checkpoint before each node (not implemented)

Status: proposal, 2026-10-26. Source: Tectic code audit 2026-10-26, finding 7.

## The problem

The sequential executor runs a node and only then saves a `Complete` checkpoint naming the next
node (`graph.rs`, `run_inner`). Nothing is written before `node.call`. A crash between the call
returning and that save makes `resume` run the same node again. For a node with a side effect
(a model call, a payment, an email) that effect happens twice.

`CrashRecoveryPolicy` exists for exactly this, but it only ever fires on a checkpoint whose status
is `Running`, and the executor never writes one: the only `Running` writes are the stores'
`claim_interrupt`. So the policy is dead in practice, and `NodeContext.execution_id`, the
intended idempotency key, has no consumer in Tectic.

## Proposal

Write `save_state_bound(.., next_node = current, CheckpointStatus::Running, ..)` immediately
before `node.call`, then the existing `Complete` save after it. On resume the policy then sees
`Running` and applies `ResetToPending` (re-run, as today), `FailFast` (stop and ask) or
`SkipAndContinue`.

## Why it is not in 0.12.1

- **One more durable write per node.** Every sequential step doubles its checkpoint writes. With
  the head CAS (`CheckpointProof`) each write is also a conflict point.
- **Behaviour change for every consumer.** Today a crashed run silently re-runs; with this change
  the default policy decides. Each consumer has to choose its policy deliberately, and Tectic's
  27 takeln consumers set none today.
- **It does not make side effects idempotent by itself.** `ResetToPending` still re-runs the
  node. The real fix is that side-effecting nodes claim `execution_id` in an effect ledger
  (Tectic already has `effect_ledger::claim` for tool calls and deliveries); the `Running`
  checkpoint only makes the crash visible.

## Recommendation

Ship it behind an opt-in (`Graph::with_pre_execution_checkpoint(true)`), default off, together
with a documented `execution_id` contract for side-effecting nodes. Switch the default in a
minor release once consumers have chosen policies. Tectic's billing is already exactly-once
(`deduct_final` under `run_lock`), so the remaining cost of not doing this is a repeated
provider call after a crash.
