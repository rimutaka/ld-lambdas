use chrono::Utc;
use dynomite::Item;
use serde::{Deserialize, Serialize};
use tokio_postgres::Row;
use uuid::Uuid;

/// Corresponds to table t_list_item
#[derive(Item, Serialize, Deserialize, Debug)]
pub(crate) struct t_list_item {
    pub liid: Uuid,
    pub parent_lid: Uuid,
    pub child_lid: Option<Uuid>,
    pub origin_liid: Option<Uuid>,
    pub origin_lid: Option<Uuid>,
    pub top_liid: Option<Uuid>,
    pub top_lid: Option<Uuid>,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub created_on_utc: chrono::DateTime<Utc>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
    pub title: Option<String>,
    pub description: Option<String>,
}

#[derive(Item, Default, Debug, Serialize, Deserialize)]
pub(crate) struct t_item {
    #[dynomite(partition_key)]
    pub liid: Uuid,
    pub title: Option<String>,
    //pub description: Option<String>,
}
/*
impl Attribute for t_item {
    fn into_attr(self: Self) -> AttributeValue {
        AttributeValue {
            ..AttributeValue::default()
        }
    }
    fn from_attr(_value: AttributeValue) -> Result<Self, dynomite::AttributeError> {

 let z = t_item {
     liid : Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap(),
     title: Some("some title".into()),
     description: None
 };
 Ok(z)

       // let x = value.m.ok_or(dynomite::AttributeError::InvalidType)?;

        // x.into_iter().map(|(s,a)| Attribute::from_attr(a))
    }
}
*/
#[derive(Item, Default, Debug, Serialize, Deserialize)]
pub(crate) struct t_list {
    #[dynomite(partition_key)]
    pub lid: Uuid,
    pub title: Option<String>,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub items: Option<Vec<t_item>>,
}

impl From<&Row> for t_list_item {
    /// Creates a new structure from tokio_postgres::Row
    fn from(row: &Row) -> Self {
        Self {
            liid: row.get("liid"),
            parent_lid: row.get("parent_lid"),
            child_lid: row.get("child_lid"),
            origin_liid: row.get("origin_liid"),
            origin_lid: row.get("origin_lid"),
            top_liid: row.get("top_liid"),
            top_lid: row.get("top_lid"),
            user_id: row.get("user_id"),
            org_id: row.get("org_id"),
            created_on_utc: row.get("created_on_utc"),
            validated_on_utc: row.get("validated_on_utc"),
            title: None,
            description: None,
        }
    }
}
