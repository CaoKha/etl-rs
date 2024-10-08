use super::base::DbBmc;
use crate::ctx::Ctx;
use crate::generate_common_bmc_fns;
use crate::model::modql_utils::time_to_sea_value;
use crate::model::{base, ModelManager, Result};
use lib_utils::time::Rfc3339;
use modql::field::Fields;
use modql::filter::{FilterNodes, ListOptions, OpValsInt64, OpValsString, OpValsValue};
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use sqlx::types::time::OffsetDateTime;
use sqlx::FromRow;

#[serde_as]
#[derive(Debug, Clone, Fields, FromRow, Serialize)]
pub struct Agent {
    pub id: i64,
    // -- Relations
    pub owner_id: i64,

    // -- Properties
    pub name: String,
    pub ai_provider: String,
    pub ai_model: String,

    // -- Timestamps
    pub cid: i64,
    #[serde_as(as = "Rfc3339")]
    pub ctime: OffsetDateTime,
    pub mid: i64,
    #[serde_as(as = "Rfc3339")]
    pub mtime: OffsetDateTime,
}

#[derive(Fields, Deserialize)]
pub struct AgentForCreate {
    pub name: String,
}

#[derive(Fields, Deserialize)]
pub struct AgentForUpdate {
    pub name: Option<String>,
}

#[derive(FilterNodes, Default, Deserialize)]
pub struct AgentFilter {
    pub id: Option<OpValsInt64>,
    pub name: Option<OpValsString>,
    pub cid: Option<OpValsInt64>,
    #[modql(to_sea_value_fn = "time_to_sea_value")]
    pub ctime: Option<OpValsValue>,
    pub mid: Option<OpValsInt64>,
    #[modql(to_sea_value_fn = "time_to_sea_value")]
    pub mtime: Option<OpValsValue>,
}

pub struct AgentBmc;

impl DbBmc for AgentBmc {
    const TABLE: &'static str = "agent";

    fn has_owner_id() -> bool {
        true
    }
}

generate_common_bmc_fns!(
    Bmc: AgentBmc,
    Entity: Agent,
    ForCreate: AgentForCreate,
    ForUpdate: AgentForUpdate,
    Filter: AgentFilter,
);

#[cfg(test)]
mod tests {
    type Error = Box<dyn core::error::Error>;
    type Result<T> = core::result::Result<T, Error>;
    use super::*;
    use crate::_dev_utils::{self, clean_agents, seed_agent};
    use serde_json::json;
    // use serde_json::json;
    use serial_test::serial;

    #[serial]
    #[tokio::test]
    async fn test_create_ok() -> Result<()> {
        // -- Setup & Fixtures
        let mm = _dev_utils::init_test().await;
        let ctx = Ctx::root_ctx();
        let fx_name = "test_create_ok agent 01";

        // -- Execute
        let fx_agent_c = AgentForCreate {
            name: fx_name.to_string(),
        };
        let agent_id = AgentBmc::create(&ctx, &mm, fx_agent_c).await?;

        // -- Check
        let agent = AgentBmc::get(&ctx, &mm, agent_id).await?;
        assert_eq!(agent.name, fx_name);

        // -- Clean
        let count = clean_agents(&ctx, &mm, "test_create_ok").await?;
        assert_eq!(count, 1, "Should have cleaned only 1 agent");
        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_create_many_ok() -> Result<()> {
        // -- Setup & Fixtures
        let mm = _dev_utils::init_test().await;
        let ctx = Ctx::root_ctx();
        let fx_name = "test_create_many_ok agent 01";

        // -- Exec
        let fx_agent_c = AgentForCreate {
            name: fx_name.to_string(),
        };
        let fx_agent_c2 = AgentForCreate {
            name: fx_name.to_string(),
        };
        let agent_ids = AgentBmc::create_many(&ctx, &mm, vec![fx_agent_c, fx_agent_c2]).await?;
        let agent_filter: AgentFilter = serde_json::from_value(json!({
        "id": {"$in": agent_ids}
        }))?;
        let agents = AgentBmc::list(&ctx, &mm, Some(vec![agent_filter]), None).await?;
        assert_eq!(agents.len(), 2, "should have only retrieved 2 agents");

        Ok(())
    }

    #[serial]
    #[tokio::test]
    async fn test_update_ok() -> Result<()> {
        // -- Setup & Fixtures
        let mm = _dev_utils::init_test().await;
        let ctx = Ctx::root_ctx();

        let fx_name = "test_update_ok agent 01";
        let fx_agent_id = seed_agent(&ctx, &mm, fx_name).await?;
        let fx_name_updated = "test_update_ok agent 02 - updated";

        // -- Exec
        let fx_agent_u = AgentForUpdate {
            name: Some(fx_name_updated.to_string()),
        };
        AgentBmc::update(&ctx, &mm, fx_agent_id, fx_agent_u).await?;

        // -- Check
        let agent = AgentBmc::get(&ctx, &mm, fx_agent_id).await?;
        assert_eq!(agent.name, fx_name_updated);

        // -- Clean
        let count = clean_agents(&ctx, &mm, "test_update_ok agent").await?;
        assert_eq!(count, 1, "Should have cleaned only 1 agent");

        Ok(())
    }
}
