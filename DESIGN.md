# Streaming Balance Rollups for Eventually-Consistent Account Sets

Replace the pull-based batch recalculation of eventually-consistent (EC) account
sets with a streaming, incremental catch-up driven by the outbox.

## Problem

Cala materializes account-set balances. On the synchronous posting path, an entry
posted to a leaf folds up into every non-EC ancestor set inline, under a lock per
`(journal, set, currency)`. **EC sets are excluded** from that path and refreshed
out-of-band. Today that is a batch recalc that rescans *every* EC set
(`list_eventually_consistent_ids` → `recalculate_balances_deep`), re-folding each
set's member history above its stored `latest_seq` **watermark**. It does not
scale: work is O(#EC-sets) per cycle regardless of activity, and latency is bounded
by the cycle period.

## Design: react to the balance that moved

> A durable job consumes the outbox. On a `BalanceCreated`/`BalanceUpdated` for a
> leaf account `A`, it folds the EC sets that have `A` as a transitive member.

The whole move is **change the trigger, not the fold** — the fold, watermark,
coalescing, and lock discipline already exist and are tested. We reuse
`recalculate_balances_batch_in_op` verbatim; the new surface is an
`OutboxEventHandler`, one reverse-membership query, and job wiring.

The consumer is built on obix's `OutboxEventHandler` (v0.4): `ctx.skip()` for
non-balance events (zero-cost — the checkpoint advances lazily), and
`ctx.consume_isolated()` for each balance event, so the fold and the cursor
checkpoint commit in **one** transaction.

Three properties make this small and safe:

- **Doorbell, not data.** The event only says "A moved." The handler re-reads each
  affected set's watermark and folds member history above it, so re-delivery folds
  nothing — correctness and idempotency live in the watermark, not delivery.
- **One indexed lookup, no cascade.** `cala_account_set_member_accounts` is a
  transitive closure, so "which EC sets is `A` under?" is one query — no graph walk.
  A set folds over its transitive *leaves* directly, so a set's own balance event
  never re-triggers its parents; only leaf events drive the consumer.
- **Bursts self-collapse.** The fold pulls everything above the watermark; a burst
  of N events for a set ⇒ the first fold drains all N and advances the watermark,
  the other N−1 fold nothing. Per-set work tracks drains, not entries.

Code: `cala-ledger/src/rollup/mod.rs` (handler),
`account_set/mod.rs::recalculate_ec_ancestors_in_op` (fold entry),
`account_set/repo.rs::find_ec_ancestor_ids_in_op` (reverse lookup), wired into
`CalaLedger::init` behind the `ec_rollup_streaming` flag (default off).

## Invariants

Money code: the properties below are what the tests exist to defend.

1. **Convergence.** After activity quiesces, an EC set's balance history equals the
   inline (`Synchronous`) set's history at matching versions. (If upstream's
   per-`(set,currency)` coalescing lands, "equals" becomes "is a subsequence of" —
   versions still count each folded delta.)
2. **Diamond safety is structural.** A leaf cannot reach one set by two paths: the
   closure is `UNIQUE(set, member)` with no `ON CONFLICT`, so the edge that would
   create a diamond is rejected at add time (`MemberAlreadyAdded`). Double-counting
   is impossible because the graph that would cause it cannot be built.
3. **Monotonic watermark.** `latest_seq` only advances; a fold at `seq ≤ watermark`
   is a no-op, so at-least-once delivery and crashes cannot corrupt state.
4. **Sole writer.** The consumer only ever writes EC sets (non-EC are the poster's);
   it never double-counts against the sync path.
5. **Per-set serialization.** `cala_balance_history.seq` comes from one global
   sequence assigned at INSERT but visible only at COMMIT, so a poster can hold a
   lower seq than a concurrent fold already sees. Advancing the watermark past it
   would skip that row forever. The reused fold's per-set lock closes this; obix's
   gaplessness does not (it orders *events* on a separate sequence, while the
   watermark rides the balance-history sequence).
6. **Gapless progress.** obix delivers events in global sequence and, via
   `consume_isolated`, commits the consumer cursor in the *same* transaction as the
   fold, so the cursor never advances past an uncommitted fold.

## Key design decisions

- **A new driver, not a new mode.** `BalanceRollup::EventuallyConsistent` is
  unchanged; only *how* it refreshes changes (outbox job vs. batch cron).
- **Trigger = balance events.** They map 1:1 to the `cala_balance_history` rows the
  watermark tracks; `EntryCreated` is one level removed. Set-account events resolve
  to zero EC ancestors — harmless no-ops.
- **Concurrency reduces to the batch case.** The consumer is serial and single
  (`spawn_unique`), so consumer-vs-consumer races don't exist; only consumer-vs-
  poster remains, solved by the reused locked fold. Even a job-runner liveness edge
  (a paused poller double-running) is absorbed by the lock + watermark no-op.

## Architecture (hexagonal fit)

The change respects the dependency rule — infrastructure points at the domain,
never the reverse:

- **Domain** (`account_set`, `balance`): the fold, the reverse-membership query,
  and `recalculate_ec_ancestors_in_op`. No `obix`/`job` imports; the entry point
  takes a generic `es_entity::AtomicOperation`, not an obix type — so even the
  transaction handle is an abstraction the domain owns, and the adapter adapts *to*
  it.
- **Inbound (driving) adapter** (`rollup::EcRollupHandler`): translates obix balance
  events into that domain call. No business logic or SQL — pure translation.
- **Composition root** (`CalaLedger::init`): wires the adapter to obix + the `job`
  runtime, behind the `ec_rollup_streaming` flag.

`obix` stays confined to the edges (`outbox` publisher, `rollup` consumer, `init`
wiring). Following cala/es-entity, domain services stay **concrete** — no port trait
is introduced, because the codebase uses none; the port that matters (persistence)
is the es-entity repo plus the `AtomicOperation` boundary. The result: the domain is
unchanged and infra-free, and streaming is added as one thin driving adapter.

## Known limitations

- **Cold start replays the outbox.** A fresh consumer starts at cursor 0 and walks
  history before reaching live events. Correct (folds past the watermark are
  no-ops) but a real deploy cost. Recommended rollout:
  `recalculate_all_eventually_consistent` once to seed watermarks, then start the
  consumer.
- **Coalescing headroom.** obix 0.4 offers a native batch accumulator
  (`type Batch` + `collect_with` / `flush`) for keyed coalescing folds. This
  handler uses per-event isolated ops (bursts still collapse via the watermark);
  moving to a batch accumulator would coalesce a burst into one transaction — a
  clean follow-up, not required for correctness.
- **Membership changes** are bounded by the existing rule that forbids adding/
  removing a member that already has balance history — a new member has no backlog.

## What was built

All milestones done. Tests in `cala-ledger/tests/ec_rollup_stream.rs`.

| Milestone | Delivered | Test |
|---|---|---|
| M1 durable consumer | handler + job wiring, config flag, `shutdown()` | `consumer_persists_cursor_and_resumes_on_restart` |
| M2 the fold | reverse lookup + `recalculate_ec_ancestors_in_op` | `ec_set_converges_via_streaming_to_inline_oracle` |
| M3 depth + concurrency | nested chains, diamond rejection, race under load | `nested_ec_sets_converge_at_every_level`, `shared_leaf_diamond_is_structurally_rejected`, `concurrent_posts_converge_without_double_count_or_skip` |
| M4 catch-up + repair | `recalculate_all_eventually_consistent` backstop | `consumer_catches_up_after_downtime`, `batch_backstop_reconciles_stale_ec_set` |
| M5 scaling | streaming folds only touched sets; batch scans all | `scaling_streaming_tracks_activity_batch_tracks_set_count` |

## Non-goals

- Not changing the synchronous posting path, non-EC semantics, the fold arithmetic,
  or the balance schema.
- Not removing batch recalc — demoted to seed/repair, not deleted.
