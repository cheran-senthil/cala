use derive_builder::Builder;
use es_entity::clock::{Clock, ClockHandle};

#[derive(Builder, Clone, Debug)]
#[builder(build_fn(validate = "Self::validate"))]
pub struct CalaLedgerConfig {
    #[builder(setter(into, strip_option), default)]
    pub(super) pg_con: Option<String>,
    #[builder(setter(into, strip_option), default)]
    pub(super) max_connections: Option<u32>,
    #[builder(default)]
    pub(super) exec_migrations: bool,
    /// When true, `CalaLedger::init` boots a background job that streams ledger
    /// balance events from the outbox and incrementally catches up
    /// eventually-consistent account sets (replacing pull-based batch recalc).
    /// Default off, so existing embedders/tests are unchanged.
    #[builder(default)]
    pub(super) ec_rollup_streaming: bool,
    #[builder(setter(into, strip_option), default)]
    pub(super) pool: Option<sqlx::PgPool>,
    #[builder(setter(into), default = "Clock::handle().clone()")]
    pub(super) clock: ClockHandle,
}

impl CalaLedgerConfig {
    pub fn builder() -> CalaLedgerConfigBuilder {
        CalaLedgerConfigBuilder::default()
    }
}

impl CalaLedgerConfigBuilder {
    fn validate(&self) -> Result<(), String> {
        match (self.pg_con.as_ref(), self.pool.as_ref()) {
            (None, None) | (Some(None), None) | (None, Some(None)) => {
                return Err("One of pg_con or pool must be set".to_string())
            }
            (Some(_), Some(_)) => return Err("Only one of pg_con or pool must be set".to_string()),
            _ => (),
        }
        Ok(())
    }
}
