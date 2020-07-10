use dynomite::{
    attr_map,
    dynamodb::{
        AttributeDefinition, CreateTableInput, DynamoDb, DynamoDbClient, GetItemInput,
        KeySchemaElement, ProvisionedThroughput, PutItemInput, ScanInput,
    },
    retry::Policy,
    DynamoDbExt, FromAttributes, Item, Retries,
};
use log::{debug, error};
use simple_logger;
use tokio;
use tokio_postgres::{Error, NoTls};
use uuid::Uuid;

//use dynamodb_data;
mod structures_ddb;
mod structures_pg;
mod utils;

#[tokio::main] // By default, tokio_postgres uses the tokio crate as its runtime.
async fn main() -> Result<(), Error> {
    simple_logger::init_with_level(log::Level::Debug).expect("Cannot initialise simple_logger");
    debug!("main started");
    debug!("");

    // prepare some constants
    let user_id = uuid::Uuid::parse_str("dbc44eaa-364f-4a4f-b25e-15218c7928a7").unwrap();
    let list_title = "My test list X".to_string();
    let lid = Uuid::new_v4();

    // Connect to the database.
    let pg_client = utils::get_pg_client().await;

    // create a new list
    let pg_list_template = structures_pg::t_list::new(lid.clone(), user_id);
    let pg_list =
        structures_pg::structures_pg_impl::put_t_list(&pg_list_template, &pg_client).await;

    // exit if there is no list
    if pg_list.is_none() {
        error!(
            "Failed to create a new list for user {} / lid {}",
            user_id, pg_list_template.lid
        );
        return Ok(());
    }

    // create a new list in DDB
    let ddb_list_template = structures_ddb::LdList::new(list_title, pg_list.unwrap());

    debug!("list created: {:?}", ddb_list_template);
    debug!("");

    // print the list as JSON
    let json = serde_json::to_string(&ddb_list_template).expect("Cannot convert Row to Json");
    println!("Json - single item: {}", json);
    println!();

    /*
        // create new items
        let pg_list_item_1 = structures_pg::t_list_item::new(pg_list.lid);
        let pg_list_item_2 = structures_pg::t_list_item::new(pg_list.lid);
        let pg_list_item_3 = structures_pg::t_list_item::new(pg_list.lid);
        debug!("dummy item created: {:?}", pg_list_item_1);
        println!();

        let pg_list_item_1p =
            structures_pg::structures_pg_impl::put_t_list_item(&pg_list_item_1, &client).await;
        let pg_list_item_2p =
            structures_pg::structures_pg_impl::put_t_list_item(&pg_list_item_2, &client).await;
        let pg_list_item_3p =
            structures_pg::structures_pg_impl::put_t_list_item(&pg_list_item_3, &client).await;
        debug!("pg item created: {:?}", pg_list_item_1p);
        println!();

        // get single item
        let pg_list_item_1g = structures_pg::structures_pg_impl::get_t_list_item(
            pg_list_item_1p.clone().unwrap().liid,
            &client,
        )
        .await;
        debug!("pg item retrieved: {:?}", pg_list_item_1g);
        println!();

        // print single item as JSON
        let json = serde_json::to_string(&pg_list_item_1g).expect("Cannot convert Row to Json");
        println!("Json - single item: {}", json);
        println!();

        // get all items for the list
        let items_get = structures_pg::structures_pg_impl::get_t_list_items(pg_list.lid, &client).await;
        debug!(
            "pg items retrieved: {}",
            items_get.as_ref().map(|itg| itg.len()).unwrap_or_else(|| 0)
        );
        println!();

        // print JSON for all items
        let json = serde_json::to_string(&items_get).expect("Cannot convert Rows to Json");
        println!("Json - all list items: {}", json);
        println!();

        // get a single list
        let list_get = structures_pg::structures_pg_impl::get_t_list(pg_list.un.lid, &client).await;
        debug!("pg list retrieved: {:?}", list_get);
        println!();

        // print JSON for single list
        let json = serde_json::to_string(&list_get).expect("Cannot convert Rows to Json");
        println!("Json - single list: {}", json);
        println!();

    */

    let ddb_client = rusoto_dynamodb::DynamoDbClient::new(rusoto_core::Region::UsEast1);
    debug!("ddb_client created");

    match ddb_list_template.save_in_ddb(&ddb_client).await {
        Ok(ddb_list) => {
            println!(
                "Clean JSON from DDB: {}",
                serde_json::to_value(ddb_list).unwrap().to_string()
            );
        }
        Err(msg) => println!("Something went wrong: {}", msg),
    }

    // add a new item to the list
    let liid = Uuid::new_v4();
    let list_item_from_ui = structures_ddb::LdListItem {
        liid: liid,
        title: "New item".to_string(),
        description: Some("Some long description".to_string()),
        rel: structures_pg::t_list_item::new(liid.clone(), lid.clone()),
    };

    match structures_ddb::LdListItem::put_list_item_ddb (list_item_from_ui, &ddb_client, &pg_client).await {
        Ok(ddb_list_item) => {
            println!(
                "Clean JSON from DDB: {}",
                serde_json::to_value(ddb_list_item).unwrap().to_string()
            );
        }
        Err(msg) => println!("Something went wrong: {}", msg),
    }

    Ok(())
}
