use crate::structures_pg::*;
use log::{self, debug, error};
use tokio_postgres::{Client, Row};
use uuid::Uuid;

// ===== From<&Row> trait implementation =====

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
        }
    }
}

impl From<&Row> for t_list {
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

impl From<&Row> for t_user {
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

impl t_list {
    /// Only `lid` and `created_on_utc` are set.
    pub(crate) fn new(lid: Uuid, user_id: Uuid) -> Self {
        Self {
            lid,
            user_id: Some(user_id),
            org_id: None,
            created_on_utc: chrono::Utc::now(),
            validated_on_utc: None,
        }
    }
}

impl t_list_item {
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
pub(crate) async fn get_t_list_item(liid: Uuid, client: &Client) -> Option<t_list_item> {
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
        1 => Some(t_list_item::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_get_tlistitem returned multiple rows ({}) for {}",
                row_count, liid
            );
            Some(t_list_item::from(&rows[0]))
        }
    }
}

/// Returns the full list of items per list
pub(crate) async fn get_t_list_items(lid: Uuid, client: &Client) -> Option<Vec<t_list_item>> {
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
    let x: Vec<t_list_item> = rows.iter().map(|r| t_list_item::from(r)).collect();
    debug!("Rows collected: {}", x.len());
    Some(x)
}

/// Returns a single t_list from PG as a structure.
pub(crate) async fn get_t_list(lid: Uuid, client: &Client) -> Option<t_list> {
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
        1 => Some(t_list::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_get_tlistitem returned multiple rows ({}) for {}",
                row_count, lid
            );
            Some(t_list::from(&rows[0]))
        }
    }
}

/// Returns a single t_user from PG as a structure. Use either 1 param + None or both params from the same user.
/// The DB will return nothing if both params do not match on the same user.
pub(crate) async fn get_t_user(
    user_id: Option<Uuid>,
    user_email: Option<String>,
    client: &Client,
) -> Option<t_user> {
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
        1 => Some(t_user::from(&rows[0])),
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
pub(crate) async fn put_t_list_item(item: &t_list_item, client: &Client) -> Option<t_list_item> {
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
        1 => Some(t_list_item::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_put_tlistitem returned multiple rows ({}) for {}",
                row_count, item.liid
            );
            Some(t_list_item::from(&rows[0]))
        }
    }
}

/// Upserts a single t_list from struct into PG.
pub(crate) async fn put_t_list(list: &t_list, client: &Client) -> Option<t_list> {
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
        1 => Some(t_list::from(&rows[0])),
        0 => {
            debug!("no rows - returning None.");
            return None;
        }
        _ => {
            error!(
                "ld_put_tlist returned multiple rows ({}) for {}",
                row_count, list.lid
            );
            Some(t_list::from(&rows[0]))
        }
    }
}

pub(crate) async fn put_t_user(user_email: &String, client: &Client) -> Option<t_user> {
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
        1 => Some(t_user::from(&rows[0])),
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





