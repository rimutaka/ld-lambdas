use log::{debug, error};
use rusoto_dynamodb::{AttributeValue, GetItemInput, PutItemInput};
use std::collections::HashMap;
use std::env::var;
use tokio_postgres::{NoTls};
use uuid::Uuid;

/// Load DB Config from env variables
pub(crate) fn load_db_config() -> String {
    // list of env vars required to connect to the DB
    const EV_DB_NAME: &'static str = "DB_NAME";
    const EV_DB_USER: &'static str = "DB_USER";
    const EV_DB_PWD: &'static str = "DB_PWD";
    const EV_DB_HOST: &'static str = "DB_HOST";

    // extract values
    let db_name = var(EV_DB_NAME).expect((format!("Env var {}", EV_DB_NAME)).as_str());
    let db_user = var(EV_DB_USER).expect((format!("Env var {}", EV_DB_USER)).as_str());
    let db_pwd = var(EV_DB_PWD).expect((format!("Env var {}", EV_DB_PWD)).as_str());
    let db_host = var(EV_DB_HOST).expect((format!("Env var {}", EV_DB_HOST)).as_str());

    // build a connection string if all values are present
    let conf = format!(
        "host={} dbname={} user={} password='{}' connect_timeout=5",
        db_host, db_name, db_user, db_pwd
    );

    return conf;
}

/// Builds GetItemInput from the key and the table name
pub(crate) fn build_ddb_get_input(table_key: &str, key_value: &Uuid, table: &str) -> GetItemInput {
    let mut key: HashMap<String, AttributeValue> = HashMap::new();
    key.insert(
        String::from(table_key),
        AttributeValue {
            s: Some(key_value.to_string()),
            ..Default::default()
        },
    );

    GetItemInput {
        key: key,
        table_name: String::from(table),
        consistent_read: Some(true),
        ..Default::default()
    }
}

pub(crate) fn build_ddb_put_input(
    item: HashMap<String, AttributeValue>,
    table: &str,
) -> PutItemInput {
    PutItemInput {
        item: item,
        table_name: String::from(table),
        ..Default::default()
    }
}

/// Prepare a client for Postgres connection. Panics if cannot connect to the PG DB.
/// The DB settings come from env vars.
pub(crate) async fn get_pg_client() -> tokio_postgres::Client {
    // try to connect to PG
    let (client, connection) = tokio_postgres::connect(&load_db_config(), NoTls)
        .await
        .expect("Cannot connect to the DB.");

    // Spawn the object that performs the actual comms with the DB into its own thread.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("PG connection error: {}", e);
            panic!();
        }
    });
    debug!("client connected");

    // return the client to the caller
    client
}
