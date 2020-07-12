use chrono::Utc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use dynomite::Item;
use log::{self, debug, error};
use tokio_postgres::{Client, Row};


#[path = "./structures_pg_test.rs"]
pub(crate) mod tests_pg;

// PG structures

/// Corresponds to table t_list_item
#[derive(Item, Serialize, Deserialize, Debug, PartialEq, Clone)]
pub(crate) struct TListItem {
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
pub(crate) struct TList {
    #[dynomite(partition_key)]
    pub lid: Uuid,
    pub user_id: Option<Uuid>,
    pub org_id: Option<Uuid>,
    pub created_on_utc: Option<chrono::DateTime<Utc>>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct TUser {
    pub user_id: Uuid,
    pub user_email: String,
    pub org_id: Option<Uuid>,
    pub created_on_utc: chrono::DateTime<Utc>,
    pub validated_on_utc: Option<chrono::DateTime<Utc>>,
}


// ===== From<&Row> trait implementation =====

impl From<&Row> for TListItem {
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
        }
    }
}

impl From<&Row> for TList {
    /// Creates a new structure from tokio_postgres::Row
    fn from(row: &Row) -> Self {
        Self {
            lid: row.get("lid"),
            user_id: row.get("user_id"),
            org_id: row.get("org_id"),
            created_on_utc: row.get("created_on_utc"),
            validated_on_utc: row.get("validated_on_utc"),
        }
    }
}

impl From<&Row> for TUser {
    /// Creates a new structure from tokio_postgres::Row
    fn from(row: &Row) -> Self {
        Self {
            user_id: row.get("user_id"),
            user_email: row.get("user_email"),
            org_id: row.get("org_id"),
            created_on_utc: row.get("created_on_utc"),
            validated_on_utc: row.get("validated_on_utc"),
        }
    }
}

// ===== struct::new() implementation =====

impl TList {
    /// Creates a new object with only the required fields set.
    /// Not saved in the DB.
    pub(crate) fn new(lid: Uuid, user_id: Uuid) -> Self {
        Self {
            lid,
            user_id: Some(user_id),
            org_id: None,
            created_on_utc: None,
            validated_on_utc: None,
        }
    }
}

impl TListItem {
    /// Creates a new instance with `liid` and `parent_lid` to be saved in the DB.
    pub(crate) fn new(liid: Uuid, parent_lid: Uuid) -> Self {
        Self {
            liid,
            parent_lid,
            child_lid: None,
            origin_lid: None,
            origin_liid: None,
            top_lid: None,
            top_liid: None,
            user_id: None,
            org_id: None,
            created_on_utc: None,
            validated_on_utc: None,
        }
    }
}

// ===== GET / PUT / DEL PG data  =====

/// Returns a single t_list_item from PG as a structure.
pub(crate) async fn get_t_list_item(liid: Uuid, client: &Client) -> Option<TListItem> {
    debug!("get_t_list_item for {}", liid);

    // get the data from PG
    let rows = client
        .query("select * from ld_get_tlistitem($1::UUID)", &[&liid])
        .await
        .expect("ld_get_tlistitem query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TListItem::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_get_tlistitem returned multiple rows ({}) for {}",
                row_count, liid
            );
            Some(TListItem::from(&rows[0]))
        }
    }
}

/// Returns the full list of items per list
pub(crate) async fn get_t_list_items(lid: Uuid, client: &Client) -> Option<Vec<TListItem>> {
    debug!("get_t_list_items for {}", lid);

    // get the data from PG
    let rows = client
        .query("select * from ld_get_tlistitems($1::UUID)", &[&lid])
        .await
        .expect("ld_get_tlistitems query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);

    // exit early if no data was fetched
    if row_count == 0 {
        debug!("no rows - returning None.");
        return None;
    };

    // collect the rows in a vector
    let x: Vec<TListItem> = rows.iter().map(|r| TListItem::from(r)).collect();
    debug!("Rows collected: {}", x.len());
    Some(x)
}

/// Returns a single t_list from PG as a structure.
pub(crate) async fn get_t_list(lid: Uuid, client: &Client) -> Option<TList> {
    debug!("get_t_list for {}", lid);

    // get the data from PG
    let rows = client
        .query("select * from ld_get_tlist($1::UUID)", &[&lid])
        .await
        .expect("ld_get_tlist query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TList::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_get_tlistitem returned multiple rows ({}) for {}",
                row_count, lid
            );
            Some(TList::from(&rows[0]))
        }
    }
}

/// Returns a single t_user from PG as a structure. Use either 1 param + None or both params from the same user.
/// The DB will return nothing if both params do not match on the same user.
pub(crate) async fn get_t_user(
    user_id: Option<Uuid>,
    user_email: Option<String>,
    client: &Client,
) -> Option<TUser> {
    debug!(
        "get_t_list for user_id {} / email {}",
        user_id.clone().unwrap_or_else(|| Uuid::default()),
        user_email.clone().unwrap_or_else(|| "none".to_string())
    );

    // get the data from PG
    let rows = client
        .query(
            "select * from ld_get_tuser($1::UUID, $2::varchar)",
            &[&user_id, &user_email],
        )
        .await
        .expect("ld_get_tlist query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TUser::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            None
        }
        _ => {
            error!("ld_get_tuser returned multiple rows {}", row_count);
            None
        }
    }
}

/// Upserts a single item from a struct to an existing PG list
pub(crate) async fn put_t_list_item(item: &TListItem, client: &Client) -> Option<TListItem> {
    debug!("put_t_list_item for {}", item.liid);

    // get the data from PG
    let rows = client
        .query(
            "select * from ld_put_tlistitem($1::UUID, $2::UUID)",
            &[&item.parent_lid, &item.liid],
        )
        .await
        .expect("ld_put_tlistitem query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TListItem::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_put_tlistitem returned multiple rows ({}) for {}",
                row_count, item.liid
            );
            Some(TListItem::from(&rows[0]))
        }
    }
}

/// Upserts a single t_list from struct into PG.
pub(crate) async fn put_t_list(list: &TList, client: &Client) -> Option<TList> {
    debug!("put_t_list for {}", list.lid);

    // get the data from PG
    let rows = client
        .query(
            "select * from ld_put_tlist($1::UUID, $2::UUID)",
            &[&list.lid, &list.user_id],
        )
        .await
        .expect("ld_put_tlist query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TList::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_put_tlist returned multiple rows ({}) for {}",
                row_count, list.lid
            );
            Some(TList::from(&rows[0]))
        }
    }
}

pub(crate) async fn put_t_user(user_email: &String, client: &Client) -> Option<TUser> {
    debug!("ld_put_tuser for {}", user_email);

    // get the data from PG
    let rows = client
        .query("select * from ld_put_tuser($1::varchar)", &[user_email])
        .await
        .expect("ld_put_tuser query failed");

    // check if the result makes sense
    let row_count = rows.len();
    debug!("Rows: {}", row_count);
    match row_count {
        1 => Some(TUser::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            None
        }
        _ => {
            error!("ld_put_tuser returned multiple rows {}", row_count);
            None
        }
    }
}

/// Deletes a single item from an existing PG list
pub(crate) async fn del_t_list_item(liid: Uuid, client: &Client) {
    debug!("del_t_list_item for {}", liid);

    // get the data from PG
    client
        .query("select * from ld_del_tlistitem($1::UUID)", &[&liid])
        .await
        .expect("ld_del_tlistitem query failed");
}

/// Deletes a single list with all child items in PG. Other linked lists are not affected.
pub(crate) async fn del_t_list(lid: Uuid, client: &Client) {
    debug!("ld_del_tlist for {}", lid);

    // get the data from PG
    client
        .query("select * from ld_del_tlist($1::UUID)", &[&lid])
        .await
        .expect("ld_del_tlist query failed");
}

/// Delete a single user from PG 
pub(crate) async fn del_t_user(user_id: Uuid, client: &Client) {
    debug!("ld_del_tuser for {}", user_id);

    // get the data from PG
    client
        .query("select * from ld_del_tuser($1::UUID)", &[&user_id])
        .await
        .expect("ld_del_tuser query failed");
}



