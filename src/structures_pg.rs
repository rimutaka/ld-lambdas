use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use dynomite::Item;

#[path = "./structures_pg_impl.rs"]
pub(crate) mod structures_pg_impl;

#[path = "./structures_pg_test.rs"]
pub(crate) mod structures_pg_test;

/// A list of possible DB errors from PG or DDB
pub(crate) enum DbOpError {
    DoNotRetry(String),
    CanRetry,
}

// PG structures

/// Corresponds to table t_list_item
#[derive(Item, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct t_list_item {
    #[dynomite(partition_key)]
    pub liid: Uuid,
    pub parent_lid: Uuid,
    pub child_lid: Option<Uuid>,
    pub origin_liid: Option<Uuid>,
    pub origin_lid: Option<Uuid>,
    pub top_liid: Option<Uuid>,
    pub top_lid: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub created_on_utc: Option<chrono::DateTime<Utc>>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
}

/// Corresponds to table t_list
#[derive(Item, Serialize, Deserialize, Debug)]
pub(crate) struct t_list {
    #[dynomite(partition_key)]
    pub lid: Uuid,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub created_on_utc: chrono::DateTime<Utc>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct t_user {
    pub user_id: Uuid,
    pub user_email: String,
    pub org_id: Option<Uuid>,
    pub created_on_utc: chrono::DateTime<Utc>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
}




