use log::{debug, error};
use rusoto_dynamodb::{
    AttributeValue, BatchGetItemInput, DeleteItemInput, GetItemInput, KeysAndAttributes, PutItemInput,
};
use std::collections::HashMap;
use std::env::var;
use tokio_postgres::NoTls;
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

/// Build BatchGetItemInput from a list of keys. Only the first 100 keys are considered
pub(crate) fn build_ddb_get_batch_input(
    table_key: &str,
    key_values: &Vec<Uuid>,
    table_name: &str,
) -> BatchGetItemInput {
    // build a list of UUID keys as a list of hashmaps
    let mut keys: Vec<HashMap<String, AttributeValue>> = Vec::new();
    for (i, kv) in key_values.iter().enumerate() {
        // only 100 can be requested as per
        // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
        if i > 99 {
            break;
        };

        // push the attribute into the array
        let key_value = kv.clone();
        let mut key: HashMap<String, AttributeValue> = HashMap::new();
        key.insert(
            String::from(table_key),
            AttributeValue {
                s: Some(key_value.to_string()),
                ..Default::default()
            },
        );
        keys.push(key);
    }

    // put it all together into RequestItems structure
    // https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html#API_BatchGetItem_RequestSyntax
    let mut request_items: HashMap<String, KeysAndAttributes> = HashMap::new();
    request_items.insert(
        table_name.into(),
        KeysAndAttributes {
            keys,
            attributes_to_get: Some(vec![
                "lid".to_string(),
                "title".to_string(),
                "description".to_string(),
                "tags".to_string(),
                "rel".to_string(),
            ]),
            ..Default::default()
        },
    );

    BatchGetItemInput {
        request_items,
        ..Default::default()
    }
}

pub(crate) fn build_ddb_put_input(item: HashMap<String, AttributeValue>, table: &str) -> PutItemInput {
    PutItemInput {
        item: item,
        table_name: String::from(table),
        ..Default::default()
    }
}

pub(crate) fn build_ddb_del_input(table_key: &str, key_value: Uuid, table: &str) -> DeleteItemInput {
    let mut key_attr: HashMap<String, AttributeValue> = HashMap::new();
    key_attr.insert(
        String::from(table_key),
        AttributeValue {
            s: Some(key_value.to_string()),
            ..Default::default()
        },
    );

    DeleteItemInput {
        key: key_attr,
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

/// Initializes `simple_logger` in a safe way to avoid panic on
/// multiple init calls.
pub(crate) fn log_init(level: log::Level) {
    match simple_logger::init_with_level(level) {
        Err(e) => {
            debug!("simple_logger::init failed {}", e.to_string());
        }

        _ => {
            debug!("simple_logger::init succeeded");
        }
    }
}
