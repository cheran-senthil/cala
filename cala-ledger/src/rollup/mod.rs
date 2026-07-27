//! Streaming, incremental catch-up for eventually-consistent (EC) account sets:
//! a durable outbox consumer that folds only the sets a balance change affects,
//! replacing the pull-based rescan (`AccountSets::recalculate_balances_deep`).
//!
//! The event is a *doorbell*, not data: on a leaf balance event the fold runs
//! for that leaf's EC ancestor sets, and the per-set watermark decides what to
//! fold — so correctness and idempotency live in the watermark, not in delivery.
//! Rationale, invariants, and milestones are in `DESIGN.md`.

use obix::{
    out::PersistentOutboxEvent, EventCtx, EventSubscription, Handled, OutboxEventHandler,
    OutboxEventJobConfig,
};

use crate::{account_set::AccountSets, outbox::OutboxEventPayload};

/// Job type name for the EC-rollup streaming consumer. Stable — it keys the
/// job's uniqueness (`spawn_unique`) and its durable cursor state.
pub const EC_ROLLUP_JOB_TYPE: &str = "cala-ec-rollup-streaming";

/// Outbox consumer that catches up eventually-consistent account sets. Its fold
/// runs on the handler's isolated `op`, which obix commits together with the
/// consumer cursor.
#[derive(Clone)]
pub struct EcRollupHandler {
    account_sets: AccountSets,
}

impl EcRollupHandler {
    pub(crate) fn new(account_sets: AccountSets) -> Self {
        Self { account_sets }
    }
}

impl OutboxEventHandler<OutboxEventPayload> for EcRollupHandler {
    /// Only the durable balance stream drives rollups; the ephemeral stream is
    /// never subscribed.
    const SUBSCRIPTION: EventSubscription = EventSubscription::PersistentOnly;

    /// No accumulator — each balance event is handled as its own isolated op.
    /// Bursts still collapse via the watermark: the first fold on a set drains
    /// all member history above it and advances the watermark, so later events
    /// for the same set fold nothing.
    type Batch = ();

    async fn handle_persistent<'inv>(
        &self,
        ctx: EventCtx<'inv>,
        event: &PersistentOutboxEvent<OutboxEventPayload>,
    ) -> Result<Handled<'inv>, Box<dyn std::error::Error + Send + Sync>> {
        // Balance events are the only doorbells for a rollup. `BalanceCreated`/
        // `BalanceUpdated` fire for both leaf and set accounts; a set account
        // resolves to zero EC ancestors (its leaves already drove the fold), so
        // its fold is a harmless no-op. Anything else costs no transaction at
        // all — `skip` advances the checkpoint lazily.
        let balance = match &event.payload {
            Some(OutboxEventPayload::BalanceCreated { balance })
            | Some(OutboxEventPayload::BalanceUpdated { balance }) => balance,
            _ => return Ok(ctx.skip()),
        };

        // One isolated op per balance event: the fold and the cursor checkpoint
        // commit together, so the streaming path keeps the batch recalc's
        // atomicity and per-set lock discipline.
        let mut op = ctx.consume_isolated().await?;
        let caught_up = self
            .account_sets
            .recalculate_ec_ancestors_in_op(&mut op, balance.account_id)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)?;

        if !caught_up.is_empty() {
            tracing::debug!(
                target: "cala_ledger.ec_rollup",
                account_id = %balance.account_id,
                n_ec_ancestors = caught_up.len(),
                "caught up EC ancestor sets from leaf balance event"
            );
        }
        Ok(op.commit())
    }
}

/// Job config for the EC-rollup consumer (registered in `CalaLedger::init`).
pub fn job_config() -> OutboxEventJobConfig {
    OutboxEventJobConfig::new(job::JobType::new(EC_ROLLUP_JOB_TYPE))
}
