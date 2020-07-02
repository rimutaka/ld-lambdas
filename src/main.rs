use chrono::Utc;
use dynomite::{FromAttributes, Item};
use rusoto_dynamodb::{AttributeValue, DynamoDb, DynamoDbClient, GetItemInput};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env::var;
use tokio;
use tokio_postgres::{Error, NoTls, Row};
use uuid::Uuid;

//use dynamodb_data;
mod structures;

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    println!("started");

    // Connect to the database.
    let (client, connection) = tokio_postgres::connect(&load_db_config(), NoTls)
        .await
        .expect("Cannot connect to the DB.");

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // Now we can execute a simple statement that just returns its parameter.
    let rows = client
        //.query("SELECT $1::TEXT", &[&"hello world"])
        .query("SELECT * from t_list_item", &[])
        .await
        .expect("Query failed");

    // And then check that we got back the same string we sent over.
    let row_count = rows.len();
    println!("Rows: {}", row_count);

    let item = structures::t_list_item::from(&rows[0]);

    let json = serde_json::to_string(&item).expect("Cannot convert Row to Json");

    //println!("Data: {}, {}, {}", item.liid, item.created_on_utc.to_rfc3339(), item.child_lid.unwrap_or(Uuid::default()));
    println!("Json: {}", json);
    /*
    let client = rusoto_s3::S3Client::new(rusoto_core::Region::default());

    let buckets = match client.list_buckets().await {
        Ok(s3_result) => s3_result,
        Err(s3_error) => panic!("S3 error {}", s3_error)
    };

    println!("Buckets {:?}", buckets);
    */

    let client = DynamoDbClient::new(rusoto_core::Region::UsEast1);

    match client
        .get_item(build_ddb_get_input("lid", &item.parent_lid, "tlist"))
        .await
    {
        Ok(output) => {
            match output.item {
                Some(x) => {
                    println!();
                    println!("Raw from DDB: {:?}", x);
                    println!();

                    let y: structures::t_list =
                    structures::t_list::from_attrs(x).expect("Error converting DDB into struct");

                    println!("Serialized from DDB: {:?}", y);
                    println!();

                    println!("Clean JSON from DDB: {}",  serde_json::to_value(y).unwrap().to_string());
                }
                None => {
                    println!("output.item is empty");
                }
            };

            /*
                        let x = dynamodb_data::from_fields::<serde_json::Value>(output.item.expect("Empty item")).expect("dynamodb_data failed");
                        println!("Output as JSON {:?}", x);
            */
        }
        Err(error) => {
            panic!("DDB error {}", error);
        }
    }

    Ok(())
}

/// Load DB Config from env variables
fn load_db_config() -> String {
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
fn build_ddb_get_input(table_key: &str, key_value: &Uuid, table: &str) -> GetItemInput {
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
        ..Default::default()
    }
}

