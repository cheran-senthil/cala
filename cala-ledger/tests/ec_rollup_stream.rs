//! M1 gate for the streaming EC-rollup consumer.
//!
//! Validates the *durable consumer* property before any fold logic exists:
//! the outbox-driven job consumes balance events, persists its cursor, and on
//! restart **resumes from the last processed sequence** rather than replaying
//! from the start. Correctness of the rollup itself is covered in later
//! milestones; here we only assert the streaming substrate.
//!
//! We assert on the cursor the obix runner persists
//! (`job_executions.execution_state_json->>'sequence'`, keyed by job type),
//! which is exactly the "resumes gaplessly" guarantee — no test-only hooks in
//! production code required.

mod helpers;

use std::time::Duration;

use rand::distr::{Alphanumeric, SampleString};

use cala_ledger::account_set::error::AccountSetError;
use cala_ledger::rollup::EC_ROLLUP_JOB_TYPE;
use cala_ledger::{account::*, account_set::*, primitives::*, tx_template::Params, *};

/// Serialize these outbox-consumer integration tests. They are timing-sensitive
/// (they poll for the async consumer to make progress) and collectively
/// over-subscribe a single Postgres, so running them concurrently is flaky.
/// This mirrors obix's own `#[file_serial]` handler tests — deterministic, no
/// extra dependency. Each test takes this guard as its first statement.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Dedicated database so the consumer starts against an **empty outbox** — the
/// shared `PG_CON` database accumulates a large outbox backlog across the whole
/// suite, and a cold-start consumer (cursor 0) would grind through all of it
/// before reaching this test's events. Isolation keeps the test fast and
/// deterministic. (Cold-start-over-a-large-backlog is a real concern addressed
/// as a seeding/repair step in a later milestone, not here.)
async fn init_isolated_pool() -> anyhow::Result<sqlx::PgPool> {
    init_isolated_pool_with(10).await
}

async fn init_isolated_pool_with(max_connections: u32) -> anyhow::Result<sqlx::PgPool> {
    let pg_con = std::env::var("PG_CON")?;
    let admin_pool = sqlx::PgPool::connect(&pg_con).await?;
    let db_name = format!(
        "ec_rollup_stream_{}",
        Alphanumeric
            .sample_string(&mut rand::rng(), 12)
            .to_lowercase()
    );
    sqlx::query(&format!(r#"CREATE DATABASE "{db_name}""#))
        .execute(&admin_pool)
        .await?;
    let (base, _) = pg_con
        .rsplit_once('/')
        .expect("PG_CON has no database path");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(max_connections)
        .acquire_timeout(Duration::from_secs(60))
        .connect(&format!("{base}/{db_name}"))
        .await?;
    sqlx::migrate!().run(&pool).await?;
    Ok(pool)
}

/// The consumer's durable cursor, or `None` if it hasn't recorded one yet.
async fn cursor_seq(pool: &sqlx::PgPool) -> anyhow::Result<Option<i64>> {
    let row: Option<(Option<i64>,)> = sqlx::query_as(
        "SELECT (execution_state_json->>'sequence')::bigint
         FROM job_executions WHERE job_type = $1",
    )
    .bind(EC_ROLLUP_JOB_TYPE)
    .fetch_optional(pool)
    .await?;
    Ok(row.and_then(|(s,)| s))
}

/// Highest sequence currently in the outbox — the catch-up target.
async fn max_outbox_seq(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let (seq,): (Option<i64>,) =
        sqlx::query_as("SELECT MAX(sequence) FROM cala_persistent_outbox_events")
            .fetch_one(pool)
            .await?;
    Ok(seq.unwrap_or(0))
}

/// Count of `jobs` rows of our type — must stay 1 (spawn_unique / single consumer).
async fn job_row_count(pool: &sqlx::PgPool) -> anyhow::Result<i64> {
    let (n,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs WHERE job_type = $1")
        .bind(EC_ROLLUP_JOB_TYPE)
        .fetch_one(pool)
        .await?;
    Ok(n)
}

/// Poll until `cond(cursor)` holds, or fail after `timeout`.
async fn wait_for_cursor<F>(
    pool: &sqlx::PgPool,
    timeout: Duration,
    mut cond: F,
) -> anyhow::Result<Option<i64>>
where
    F: FnMut(Option<i64>) -> bool,
{
    let start = std::time::Instant::now();
    loop {
        let c = cursor_seq(pool).await?;
        if cond(c) {
            return Ok(c);
        }
        if start.elapsed() > timeout {
            anyhow::bail!("timeout waiting for cursor; last = {c:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll until the EC set's balance for `currency` matches the inline set's
/// version (i.e. the streaming consumer has caught the EC set up), or fail.
/// Returns nothing; callers re-`find` both balances afterward to assert.
async fn wait_for_ec_version(
    cala: &CalaLedger,
    journal_id: JournalId,
    ec_set_id: AccountSetId,
    currency: Currency,
    target_version: u32,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    loop {
        if let Ok(bal) = cala.balances().find(journal_id, ec_set_id, currency).await {
            if bal.details.version >= target_version {
                return Ok(());
            }
        }
        if start.elapsed() > timeout {
            anyhow::bail!("timeout waiting for EC set to reach version {target_version}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the consumer has processed every event currently in the outbox
/// (cursor >= current max sequence) — i.e. it has fully drained. Guarantees all
/// balance events from prior posts have been folded before we assert.
async fn wait_for_drain(pool: &sqlx::PgPool, timeout: Duration) -> anyhow::Result<()> {
    let target = max_outbox_seq(pool).await?;
    wait_for_cursor(pool, timeout, |c| c.map(|s| s >= target).unwrap_or(false)).await?;
    Ok(())
}

async fn post_n(
    cala: &CalaLedger,
    tx_code: &str,
    journal_id: JournalId,
    sender: AccountId,
    recipient: AccountId,
    n: usize,
) {
    for _ in 0..n {
        let mut params = Params::new();
        params.insert("journal_id", journal_id.to_string());
        params.insert("sender", sender);
        params.insert("recipient", recipient);
        cala.post_transaction(TransactionId::new(), tx_code, params)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn consumer_persists_cursor_and_resumes_on_restart() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let pool = init_isolated_pool().await?;
    let timeout = Duration::from_secs(10);

    // Shared fixture: journal, accounts, an EC set with the recipient as member,
    // and a posting template.
    let (journal_id, sender_id, recipient_id, tx_code) = {
        let cala = CalaLedger::init(
            CalaLedgerConfig::builder()
                .pool(pool.clone())
                .exec_migrations(false)
                .build()?,
        )
        .await?;

        let journal = cala.journals().create(helpers::test_journal()).await?;
        let (sender, receiver) = helpers::test_accounts();
        let sender = cala.accounts().create(sender).await?;
        let recipient = cala.accounts().create(receiver).await?;

        let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
        cala.tx_templates()
            .create(helpers::currency_conversion_template(&tx_code))
            .await?;

        let ec_set = NewAccountSet::builder()
            .id(AccountSetId::new())
            .name("EC streaming set")
            .journal_id(journal.id())
            .balance_rollup(BalanceRollup::EventuallyConsistent)
            .build()
            .unwrap();
        let ec_set = cala.account_sets().create(ec_set).await?;
        cala.account_sets()
            .add_member(ec_set.id(), recipient.id())
            .await?;

        (journal.id(), sender.id(), recipient.id(), tx_code)
    };

    // ── First run: stream on, post activity, let the cursor advance ──────────
    let cursor_after_first;
    {
        let cala = CalaLedger::init(
            CalaLedgerConfig::builder()
                .pool(pool.clone())
                .exec_migrations(false)
                .ec_rollup_streaming(true)
                .build()?,
        )
        .await?;

        post_n(&cala, &tx_code, journal_id, sender_id, recipient_id, 3).await;
        let target = max_outbox_seq(&pool).await?;
        assert!(target > 0, "outbox should have events");

        // Consumer catches up to the latest outbox sequence.
        cursor_after_first =
            wait_for_cursor(&pool, timeout, |c| c.map(|s| s >= target).unwrap_or(false))
                .await?
                .expect("cursor present");

        assert_eq!(job_row_count(&pool).await?, 1, "exactly one consumer job");

        cala.shutdown().await?;
    }

    // ── Restart: a fresh CalaLedger re-registers the SAME durable job ────────
    {
        let cala = CalaLedger::init(
            CalaLedgerConfig::builder()
                .pool(pool.clone())
                .exec_migrations(false)
                .ec_rollup_streaming(true)
                .build()?,
        )
        .await?;

        // Still exactly one job — re-registration is idempotent (spawn_unique).
        assert_eq!(
            job_row_count(&pool).await?,
            1,
            "restart must not fork a second job"
        );

        // The cursor resumed at its persisted position, not reset to 0.
        let resumed = cursor_seq(&pool).await?.expect("cursor survives restart");
        assert_eq!(
            resumed, cursor_after_first,
            "consumer must resume from the last processed sequence"
        );

        // New activity advances the cursor further — the resumed job keeps going.
        post_n(&cala, &tx_code, journal_id, sender_id, recipient_id, 2).await;
        let target = max_outbox_seq(&pool).await?;
        let cursor_after_second =
            wait_for_cursor(&pool, timeout, |c| c.map(|s| s >= target).unwrap_or(false))
                .await?
                .expect("cursor present");

        assert!(
            cursor_after_second > cursor_after_first,
            "cursor must advance past its pre-restart position ({cursor_after_second} > {cursor_after_first})"
        );

        cala.shutdown().await?;
    }

    Ok(())
}

/// M2 gate: an EC set caught up **only by the streaming consumer** (we never
/// call `recalculate_balances`) must converge to the same balance as an inline
/// (`Synchronous`) set with the same member — and its balance history must be
/// the inline set's history at matching versions. This is invariant 1, the
/// convergence oracle. (On this pre-coalescing checkout the EC history is
/// row-for-row equal to inline; once coalescing lands it becomes a subsequence.)
#[tokio::test]
async fn ec_set_converges_via_streaming_to_inline_oracle() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    use cala_ledger::balance::BalanceSnapshot;
    use sqlx::Row as _;

    let btc: Currency = "BTC".parse().unwrap();
    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool().await?;
    let timeout = Duration::from_secs(10);

    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let (sender, receiver) = helpers::test_accounts();
    let sender = cala.accounts().create(sender).await?;
    let recipient = cala.accounts().create(receiver).await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::currency_conversion_template(&tx_code))
        .await?;

    // Inline set is the oracle; EC set is maintained only by the stream.
    let inline_set = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("Inline oracle set")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::Synchronous)
                .build()
                .unwrap(),
        )
        .await?;
    let ec_set = cala
        .account_sets()
        .create(
            NewAccountSet::builder()
                .id(AccountSetId::new())
                .name("EC streaming set")
                .journal_id(journal.id())
                .balance_rollup(BalanceRollup::EventuallyConsistent)
                .build()
                .unwrap(),
        )
        .await?;
    cala.account_sets()
        .add_member(inline_set.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(ec_set.id(), recipient.id())
        .await?;

    post_n(
        &cala,
        &tx_code,
        journal.id(),
        sender.id(),
        recipient.id(),
        3,
    )
    .await;

    // Inline set is updated synchronously by the posts; it's the target.
    let inline_btc = cala
        .balances()
        .find(journal.id(), inline_set.id(), btc)
        .await?;

    // Wait for the streaming consumer to bring the EC set to the same version —
    // note we NEVER call recalculate_balances here.
    wait_for_ec_version(
        &cala,
        journal.id(),
        ec_set.id(),
        btc,
        inline_btc.details.version,
        timeout,
    )
    .await?;

    // ── Balances converge across currencies ──────────────────────────────────
    let ec_btc = cala.balances().find(journal.id(), ec_set.id(), btc).await?;
    assert_eq!(
        inline_btc.settled(),
        ec_btc.settled(),
        "BTC settled must match"
    );
    assert_eq!(
        inline_btc.details.version, ec_btc.details.version,
        "BTC version must match"
    );

    let inline_usd = cala
        .balances()
        .find(journal.id(), inline_set.id(), usd)
        .await?;
    let ec_usd = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(
        inline_usd.settled(),
        ec_usd.settled(),
        "USD settled must match"
    );
    assert_eq!(
        inline_usd.pending(),
        ec_usd.pending(),
        "USD pending must match"
    );

    // ── History is the inline history at matching versions (invariant 1) ─────
    let inline_account_id = AccountId::from(&inline_set.id());
    let ec_account_id = AccountId::from(&ec_set.id());
    let fetch_history = |account_id: AccountId| {
        let pool = pool.clone();
        let jid = journal.id();
        let cur = btc.code();
        async move {
            sqlx::query(
                "SELECT values FROM cala_balance_history \
                 WHERE account_id = $1 AND journal_id = $2 AND currency = $3 ORDER BY version",
            )
            .bind(account_id)
            .bind(jid)
            .bind(cur)
            .fetch_all(&pool)
            .await
        }
    };
    let inline_history = fetch_history(inline_account_id).await?;
    let ec_history = fetch_history(ec_account_id).await?;

    // Index inline snapshots by version; every EC row must equal the inline row
    // at the same version (a subsequence; here full equality, pre-coalescing).
    let inline_by_version: std::collections::HashMap<u32, BalanceSnapshot> = inline_history
        .iter()
        .map(|row| {
            let snap: BalanceSnapshot =
                serde_json::from_value(row.try_get::<serde_json::Value, _>("values").unwrap())
                    .unwrap();
            (snap.version, snap)
        })
        .collect();
    assert!(
        !ec_history.is_empty(),
        "EC set must have BTC history from the stream"
    );
    for ec_row in &ec_history {
        let e: BalanceSnapshot =
            serde_json::from_value(ec_row.try_get::<serde_json::Value, _>("values")?)?;
        let i = inline_by_version
            .get(&e.version)
            .unwrap_or_else(|| panic!("no inline BTC row at v{}", e.version));
        assert_eq!(
            i.settled.dr_balance, e.settled.dr_balance,
            "settled dr @v{}",
            e.version
        );
        assert_eq!(
            i.settled.cr_balance, e.settled.cr_balance,
            "settled cr @v{}",
            e.version
        );
        assert_eq!(i.entry_id, e.entry_id, "entry_id @v{}", e.version);
    }

    cala.shutdown().await?;
    Ok(())
}

/// Helper: create an account with a random unique code.
async fn new_account(cala: &CalaLedger, label: &str) -> anyhow::Result<Account> {
    let code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    let acc = NewAccount::builder()
        .id(uuid::Uuid::now_v7())
        .name(format!("{label} {code}"))
        .code(code)
        .build()
        .unwrap();
    Ok(cala.accounts().create(acc).await?)
}

fn new_set(journal_id: JournalId, name: &str, rollup: BalanceRollup) -> NewAccountSet {
    NewAccountSet::builder()
        .id(AccountSetId::new())
        .name(name)
        .journal_id(journal_id)
        .balance_rollup(rollup)
        .build()
        .unwrap()
}

/// M3a — depth: a leaf under a *nested* chain of EC sets (child EC set is a
/// member of a parent EC set) brings **both** levels up to date. There is no
/// cascade: each set folds over its transitive leaves directly, so both the
/// child and the parent are direct EC ancestors of the leaf and are folded from
/// the same leaf event. Oracle: an inline chain with the same shape.
#[tokio::test]
async fn nested_ec_sets_converge_at_every_level() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool().await?;
    let timeout = Duration::from_secs(15);

    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let sender = new_account(&cala, "sender").await?;
    let recipient = new_account(&cala, "recipient").await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::simple_template_with_date_default(&tx_code))
        .await?;

    // Inline chain (oracle): leaf -> inline_child -> inline_parent.
    let inline_child = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "inline child",
            BalanceRollup::Synchronous,
        ))
        .await?;
    let inline_parent = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "inline parent",
            BalanceRollup::Synchronous,
        ))
        .await?;
    cala.account_sets()
        .add_member(inline_child.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(inline_parent.id(), inline_child.id())
        .await?;

    // EC chain (under test): leaf -> ec_child -> ec_parent.
    let ec_child = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "ec child",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;
    let ec_parent = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "ec parent",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;
    cala.account_sets()
        .add_member(ec_child.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(ec_parent.id(), ec_child.id())
        .await?;

    // Post activity.
    for _ in 0..3 {
        let mut params = Params::new();
        params.insert("journal_id", journal.id().to_string());
        params.insert("sender", sender.id());
        params.insert("recipient", recipient.id());
        params.insert("amount", rust_decimal::Decimal::from(11));
        cala.post_transaction(TransactionId::new(), &tx_code, params)
            .await?;
    }

    wait_for_drain(&pool, timeout).await?;

    // Both EC levels match their inline counterparts.
    let inline_child_bal = cala
        .balances()
        .find(journal.id(), inline_child.id(), usd)
        .await?;
    let ec_child_bal = cala
        .balances()
        .find(journal.id(), ec_child.id(), usd)
        .await?;
    assert_eq!(
        inline_child_bal.settled(),
        ec_child_bal.settled(),
        "child level must match"
    );

    let inline_parent_bal = cala
        .balances()
        .find(journal.id(), inline_parent.id(), usd)
        .await?;
    let ec_parent_bal = cala
        .balances()
        .find(journal.id(), ec_parent.id(), usd)
        .await?;
    assert_eq!(
        inline_parent_bal.settled(),
        ec_parent_bal.settled(),
        "parent level must match"
    );

    // The parent equals the child (single leaf beneath both) — sanity on the chain.
    assert_eq!(
        ec_parent_bal.settled(),
        ec_child_bal.settled(),
        "nested parent == child for one leaf"
    );

    cala.shutdown().await?;
    Ok(())
}

/// M3b — diamond safety, established structurally. A "diamond" that would let a
/// single leaf reach one ancestor set by two paths (leaf -> A, leaf -> B, and
/// both A, B -> G) is **rejected at membership-add time**: the transitive
/// closure `cala_account_set_member_accounts` is `UNIQUE(set, member)` and the
/// closure insert has no `ON CONFLICT`, so the edge that would insert
/// `G -> leaf` a second time fails with `MemberAlreadyAdded`.
///
/// This is *why* the EC rollup can never double-count a leaf: not because we
/// dedupe at fold time, but because the ledger forbids constructing the graph
/// that would double-count. The closure the fold reads is a set, not a multiset,
/// enforced at write time. (Applies to EC and inline sets identically.)
#[tokio::test]
async fn shared_leaf_diamond_is_structurally_rejected() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let pool = init_isolated_pool().await?;
    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let leaf = new_account(&cala, "shared leaf").await?;

    let a = cala
        .account_sets()
        .create(new_set(journal.id(), "A", BalanceRollup::Synchronous))
        .await?;
    let b = cala
        .account_sets()
        .create(new_set(journal.id(), "B", BalanceRollup::Synchronous))
        .await?;
    let g = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "G ec",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;

    // leaf -> A, leaf -> B, A -> G: fine so far (G's closure now holds leaf via A).
    cala.account_sets().add_member(a.id(), leaf.id()).await?;
    cala.account_sets().add_member(b.id(), leaf.id()).await?;
    cala.account_sets().add_member(g.id(), a.id()).await?;

    // B -> G would insert (G, leaf) a second time — the diamond. Must be rejected.
    match cala.account_sets().add_member(g.id(), b.id()).await {
        Err(AccountSetError::MemberAlreadyAdded) => {}
        Err(e) => panic!("expected MemberAlreadyAdded, got a different error: {e}"),
        Ok(_) => panic!("shared-leaf diamond edge was accepted; it must be rejected"),
    }

    cala.shutdown().await?;
    Ok(())
}

/// M3c — concurrency: many writers post to members of an EC set while the
/// streaming consumer folds it. Mirrors `ec_recalc_race.rs`, but catch-up is
/// driven *only* by the stream (no `recalculate_balances` calls). Once the
/// consumer drains, the EC set must equal the exact sum of all posts — no
/// double-count, no skipped row — exercising the same `nextval`-vs-visibility
/// race the batch path guards against, now on the streaming trigger.
#[tokio::test]
async fn concurrent_posts_converge_without_double_count_or_skip() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    use std::sync::Arc;

    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool_with(40).await?;
    let timeout = Duration::from_secs(30);

    const N_MEMBERS: usize = 6;
    const N_WRITERS: usize = 6;
    const POSTS_PER_WRITER: usize = 8;
    let post_amount = rust_decimal::Decimal::from(7);

    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let sender = new_account(&cala, "sender").await?;
    let mut members = Vec::with_capacity(N_MEMBERS);
    for _ in 0..N_MEMBERS {
        members.push(new_account(&cala, "member").await?);
    }
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::simple_template_with_date_default(&tx_code))
        .await?;

    let ec_set = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "EC concurrent",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;
    for m in &members {
        cala.account_sets().add_member(ec_set.id(), m.id()).await?;
    }

    let member_ids: Arc<Vec<AccountId>> = Arc::new(members.iter().map(|a| a.id()).collect());

    let mut handles = Vec::new();
    for _ in 0..N_WRITERS {
        let cala = cala.clone();
        let member_ids = member_ids.clone();
        let tx_code = tx_code.clone();
        let sender_id = sender.id();
        let journal_id = journal.id();
        handles.push(tokio::spawn(async move {
            for i in 0..POSTS_PER_WRITER {
                let recipient_id = member_ids[i % member_ids.len()];
                let mut params = Params::new();
                params.insert("journal_id", journal_id.to_string());
                params.insert("sender", sender_id);
                params.insert("recipient", recipient_id);
                params.insert("amount", post_amount);
                cala.post_transaction(TransactionId::new(), &tx_code, params)
                    .await
                    .map_err(|e| anyhow::anyhow!("post failed: {e}"))?;
            }
            Ok::<_, anyhow::Error>(())
        }));
    }
    for h in handles {
        h.await??;
    }

    // All posts committed. Let the stream drain, then the EC set must reflect
    // every post exactly — this is the assertion the watermark race would break.
    wait_for_drain(&pool, timeout).await?;

    let total_posts = N_WRITERS * POSTS_PER_WRITER;
    let expected_total = post_amount * rust_decimal::Decimal::from(total_posts);

    let ec_bal = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(
        ec_bal.settled(),
        expected_total,
        "streamed EC set must equal sum of all posts (got {}, expected {expected_total})",
        ec_bal.settled(),
    );

    // Cross-check against the actual member balances.
    let mut sum_members = rust_decimal::Decimal::ZERO;
    for m in &members {
        if let Ok(b) = cala.balances().find(journal.id(), m.id(), usd).await {
            sum_members += b.settled();
        }
    }
    assert_eq!(
        sum_members, expected_total,
        "sum of member balances must equal posts"
    );
    assert_eq!(
        ec_bal.settled(),
        sum_members,
        "EC set must equal sum of members"
    );

    cala.shutdown().await?;
    Ok(())
}

/// M4a — lag catch-up after downtime. The consumer processes some activity,
/// goes down, and MORE activity accrues while it's off. On restart it drains
/// the backlog and the EC set converges to the inline oracle. This is the
/// "incremental catch-up" property: being behind is just a longer drain, and a
/// single drained fold coalesces the whole backlog for a set.
#[tokio::test]
async fn consumer_catches_up_after_downtime() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool().await?;
    let timeout = Duration::from_secs(15);

    // Fixture with the streaming consumer OFF, so we can accrue a backlog first.
    let cala_off = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .build()?,
    )
    .await?;
    let journal = cala_off.journals().create(helpers::test_journal()).await?;
    let sender = new_account(&cala_off, "sender").await?;
    let recipient = new_account(&cala_off, "recipient").await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala_off
        .tx_templates()
        .create(helpers::simple_template_with_date_default(&tx_code))
        .await?;
    let inline_set = cala_off
        .account_sets()
        .create(new_set(journal.id(), "inline", BalanceRollup::Synchronous))
        .await?;
    let ec_set = cala_off
        .account_sets()
        .create(new_set(
            journal.id(),
            "ec",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;
    cala_off
        .account_sets()
        .add_member(inline_set.id(), recipient.id())
        .await?;
    cala_off
        .account_sets()
        .add_member(ec_set.id(), recipient.id())
        .await?;

    let post = |cala: CalaLedger, n: usize| {
        let tx_code = tx_code.clone();
        let (jid, sid, rid) = (journal.id(), sender.id(), recipient.id());
        async move {
            for _ in 0..n {
                let mut params = Params::new();
                params.insert("journal_id", jid.to_string());
                params.insert("sender", sid);
                params.insert("recipient", rid);
                params.insert("amount", rust_decimal::Decimal::from(5));
                cala.post_transaction(TransactionId::new(), &tx_code, params)
                    .await
                    .unwrap();
            }
        }
    };

    // Backlog accrues entirely while the consumer is OFF.
    post(cala_off.clone(), 4).await;

    // Bring the consumer up; it must catch the whole backlog up from behind.
    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    // More activity arrives after it's up, too.
    post(cala.clone(), 3).await;

    wait_for_drain(&pool, timeout).await?;

    let inline_bal = cala
        .balances()
        .find(journal.id(), inline_set.id(), usd)
        .await?;
    let ec_bal = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(
        inline_bal.settled(),
        ec_bal.settled(),
        "EC must catch up to inline after downtime"
    );

    cala.shutdown().await?;
    Ok(())
}

/// M4b — batch backstop coexists and reconciles. With streaming OFF, an EC set
/// goes stale (never folded). `recalculate_all_eventually_consistent` — the
/// demoted-but-not-deleted batch path — reconciles it to the inline oracle, and
/// is idempotent. This is the disaster-recovery / initial-seed tool.
#[tokio::test]
async fn batch_backstop_reconciles_stale_ec_set() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool().await?;

    // Streaming OFF the whole time.
    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .build()?,
    )
    .await?;
    let journal = cala.journals().create(helpers::test_journal()).await?;
    let sender = new_account(&cala, "sender").await?;
    let recipient = new_account(&cala, "recipient").await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::simple_template_with_date_default(&tx_code))
        .await?;
    let inline_set = cala
        .account_sets()
        .create(new_set(journal.id(), "inline", BalanceRollup::Synchronous))
        .await?;
    let ec_set = cala
        .account_sets()
        .create(new_set(
            journal.id(),
            "ec",
            BalanceRollup::EventuallyConsistent,
        ))
        .await?;
    cala.account_sets()
        .add_member(inline_set.id(), recipient.id())
        .await?;
    cala.account_sets()
        .add_member(ec_set.id(), recipient.id())
        .await?;

    for _ in 0..3 {
        let mut params = Params::new();
        params.insert("journal_id", journal.id().to_string());
        params.insert("sender", sender.id());
        params.insert("recipient", recipient.id());
        params.insert("amount", rust_decimal::Decimal::from(9));
        cala.post_transaction(TransactionId::new(), &tx_code, params)
            .await?;
    }

    // EC set is stale — no streaming consumer ran.
    assert!(
        cala.balances()
            .find(journal.id(), ec_set.id(), usd)
            .await
            .is_err(),
        "EC set should be stale with streaming off"
    );

    // Batch backstop reconciles all EC sets.
    let n = cala
        .account_sets()
        .recalculate_all_eventually_consistent()
        .await?;
    assert!(n >= 1, "at least our EC set should be reconciled");

    let inline_bal = cala
        .balances()
        .find(journal.id(), inline_set.id(), usd)
        .await?;
    let ec_bal = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(
        inline_bal.settled(),
        ec_bal.settled(),
        "backstop must reconcile EC to inline"
    );

    // Idempotent: re-running changes nothing.
    let version_before = ec_bal.details.version;
    cala.account_sets()
        .recalculate_all_eventually_consistent()
        .await?;
    let ec_bal_2 = cala.balances().find(journal.id(), ec_set.id(), usd).await?;
    assert_eq!(
        ec_bal_2.details.version, version_before,
        "backstop must be idempotent"
    );

    Ok(())
}

/// M5 — scaling shape: streaming work tracks *activity*, batch work tracks the
/// *number of EC sets*. This is the core "at scale" argument for replacing the
/// pull-based rescan.
///
/// Build a WIDE forest of `N` EC sets, each over its own idle leaf, then touch
/// exactly ONE leaf. Assert:
///  - streaming folds only the touched set — after draining, exactly one EC set
///    has a balance; the other N-1 were never folded (the consumer never looked
///    at them). Work = O(changed sets), independent of N.
///  - the batch backstop must enumerate all N EC sets to discover that N-1 are
///    idle (`recalculate_all_eventually_consistent` returns N). Work = O(N),
///    independent of activity.
/// Wall-clock for both is printed as supporting evidence (not asserted).
#[tokio::test]
async fn scaling_streaming_tracks_activity_batch_tracks_set_count() -> anyhow::Result<()> {
    let _serial = SERIAL.lock().await;
    let usd: Currency = "USD".parse().unwrap();
    let pool = init_isolated_pool_with(16).await?;
    let timeout = Duration::from_secs(30);

    const N: usize = 50;

    let cala = CalaLedger::init(
        CalaLedgerConfig::builder()
            .pool(pool.clone())
            .exec_migrations(false)
            .ec_rollup_streaming(true)
            .build()?,
    )
    .await?;

    let journal = cala.journals().create(helpers::test_journal()).await?;
    let sender = new_account(&cala, "sender").await?;
    let tx_code = Alphanumeric.sample_string(&mut rand::rng(), 32);
    cala.tx_templates()
        .create(helpers::simple_template_with_date_default(&tx_code))
        .await?;

    // N EC sets, each over its own distinct idle leaf.
    let mut ec_sets = Vec::with_capacity(N);
    let mut leaves = Vec::with_capacity(N);
    for i in 0..N {
        let leaf = new_account(&cala, &format!("leaf {i}")).await?;
        let set = cala
            .account_sets()
            .create(new_set(
                journal.id(),
                &format!("ec {i}"),
                BalanceRollup::EventuallyConsistent,
            ))
            .await?;
        cala.account_sets().add_member(set.id(), leaf.id()).await?;
        ec_sets.push(set);
        leaves.push(leaf);
    }

    // Touch exactly ONE leaf.
    let touched = 7usize;
    for _ in 0..2 {
        let mut params = Params::new();
        params.insert("journal_id", journal.id().to_string());
        params.insert("sender", sender.id());
        params.insert("recipient", leaves[touched].id());
        params.insert("amount", rust_decimal::Decimal::from(3));
        cala.post_transaction(TransactionId::new(), &tx_code, params)
            .await?;
    }

    // Streaming: let the consumer drain, then count how many EC sets have a
    // balance. Only the touched set should — the consumer never folded the rest.
    let stream_start = std::time::Instant::now();
    wait_for_drain(&pool, timeout).await?;
    let stream_elapsed = stream_start.elapsed();

    let mut sets_with_balance = 0usize;
    for s in &ec_sets {
        if cala
            .balances()
            .find(journal.id(), s.id(), usd)
            .await
            .is_ok()
        {
            sets_with_balance += 1;
        }
    }
    assert_eq!(
        sets_with_balance, 1,
        "streaming must fold ONLY the touched set ({sets_with_balance} of {N} folded)"
    );
    // And it's the right one.
    assert!(
        cala.balances()
            .find(journal.id(), ec_sets[touched].id(), usd)
            .await
            .is_ok(),
        "the touched set must be the folded one"
    );

    // Batch backstop: must enumerate ALL N EC sets to reconcile, even though
    // N-1 are idle. This is the O(N)-regardless-of-activity cost streaming avoids.
    let batch_start = std::time::Instant::now();
    let reconciled = cala
        .account_sets()
        .recalculate_all_eventually_consistent()
        .await?;
    let batch_elapsed = batch_start.elapsed();
    assert_eq!(
        reconciled, N,
        "batch must scan all {N} EC sets regardless of activity"
    );

    eprintln!(
        "M5 scaling (N={N} EC sets, 1 touched): \
         streaming folded 1 set (drain {stream_elapsed:?}); \
         batch enumerated {N} sets (recalc {batch_elapsed:?})"
    );

    cala.shutdown().await?;
    Ok(())
}
