use crate::structures_pg::{self};
use crate::utils;
use dynomite::{
    attr_map,
    dynamodb::{
        AttributeDefinition, CreateTableInput, DynamoDb, DynamoDbClient, GetItemInput,
        KeySchemaElement, ProvisionedThroughput, PutItemInput, ScanInput,
    },
    retry::Policy,
    DynamoDbExt, FromAttributes, Item, Retries,
};
use log::{self, debug, error, info, warn};
use serde::{Deserialize, Serialize};
use tokio_postgres;
use uuid::Uuid;

//#[path ="./structures_pg.rs"]
//mod structures_pg;
//#[path ="./structures_pg_impl.rs"]
//mod structures_pg_impl;

// DDB structures

const TABLE_NAME_TLIST: &str = "tlist";
const TABLE_KEY_FOR_TLIST: &str = "lid";
const ERR_MSG_SAVING_ITEM_FAILED: &str = "Failed to save this new list item in the DB.";

/// A single list item. Part of LdList.
#[derive(Item, Debug, Serialize, Deserialize)]
pub(crate) struct LdListItem {
    #[dynomite(partition_key)]
    pub title: String,
    pub description: Option<String>,
    pub rel: structures_pg::t_list_item,
}

/// A complete List structure to exchange with the front-end
#[derive(Item, Debug, Serialize, Deserialize)]
pub(crate) struct LdList {
    #[dynomite(partition_key)]
    pub lid: Uuid,
    pub title: String,
    pub description: Option<String>,
    pub tags: Option<Vec<String>>,
    pub items: Option<Vec<LdListItem>>,
    pub rel: structures_pg::t_list,
}

impl LdList {
    /// Create a new LdList struct with no items and only required fields.
    /// It is not saved in the DB.
    pub(crate) fn new(lid: Uuid, title: String, user_id: Uuid) -> LdList {
        // create a new list
        let pg_list_template = structures_pg::t_list::new(lid.clone(), user_id);

        LdList {
            lid: lid,
            title: title,
            description: None,
            tags: None,
            items: None,
            rel: pg_list_template,
        }
    }

    /// Save itself in DDB, get the latest version back and return it wrapped in Result.
    /// The `rel` section is saved in PG if none exists.
    pub(crate) async fn save_in_ddb(
        mut self,
        ddb_client: &DynamoDbClient,
        pg_client: &tokio_postgres::Client,
    ) -> Result<Option<Self>, String> {
        // this var will be used a few times
        let lid = self.lid.clone();

        debug!("save_in_ddb for {}", lid);

        // check if it's a brand-new list and needs `rel` section created in PG first
        if self.rel.created_on_utc.is_none() {
            let pg_list =
                structures_pg::structures_pg_impl::put_t_list(&self.rel, &pg_client).await;

            // exit if there is no list
            if pg_list.is_none() {
                error!(
                    "Failed to create a new list for user {} / lid {}",
                    self.rel.user_id.expect("Missing user_id"), lid
                );
                return Err("Failed to create a new list.".to_string());
            }

            // replace the placeholder list with the proper one from PG
            self.rel = pg_list.unwrap();
        }

        // put the item in DDB
        if let Err(put_err) = ddb_client
            .put_item(utils::build_ddb_put_input(self.into(), TABLE_NAME_TLIST))
            .await
        {
            error!("Failed to put_item {:?}", put_err);
            return Err("Failed to save in DDB.".to_string());
        }
        debug!("Item put in DDB.");

        // get the same record back from DDB
        LdList::get_from_ddb(&lid, &ddb_client).await
    }

    /// Retrieve a single list from DDB by ID. Should not panic.
    pub(crate) async fn get_from_ddb(
        lid: &Uuid,
        ddb_client: &DynamoDbClient,
    ) -> Result<Option<Self>, String> {
        // this var will be used a few times
        let lid = lid.clone();

        debug!("get_from_ddb for {}", lid);

        // retrieve the latest copy, which may be a bit different from what was saved
        match ddb_client
            .get_item(utils::build_ddb_get_input(
                TABLE_KEY_FOR_TLIST,
                &lid,
                TABLE_NAME_TLIST,
            ))
            .await
        {
            Ok(get_item_output) => {
                match get_item_output.item {
                    Some(output_item) => {
                        debug!("Raw from DDB: {:?}", output_item);

                        let new_self = LdList::from_attrs(output_item)
                            .expect("Error converting DDB list into LdList");

                        debug!("Serialized from DDB: {:?}", new_self);
                        return Ok(Some(new_self));
                    }
                    None => {
                        error!("Just-saved DDB item could not be retrieved - no error, no data.");
                        return Ok(None);
                    }
                };
            }
            Err(error) => {
                error!("DDB error {}", error);
                return Err("Failed to save the item. Try again.".to_string());
            }
        }
    }
}

impl LdListItem {
    /// Add a new or update an existing List Item inside its list. Updates DDB and PG in one go.
    pub(crate) async fn put_list_item_ddb(
        list_item: LdListItem,
        ddb_client: &DynamoDbClient,
        pg_client: &tokio_postgres::Client,
    ) -> Result<Self, String> {
        // get the list from DDB
        let list = LdList::get_from_ddb(&list_item.rel.parent_lid, &ddb_client).await;

        // return the error if no list exists or there were problems getting it from DDB
        if list.is_err() {
            return Err(list.unwrap_err());
        }
        let list = list.unwrap();
        if list.is_none() {
            return Err("The list for this item doesn't exist".to_string());
        }

        // this is the actual list struct that will be modified
        let mut list = list.unwrap();

        // make sure there is a container for items
        if list.items.is_none() {
            list.items = Some(Vec::new());
        }

        // try to find the right item in the existing list
        let mut is_existing_item = false;
        let ref mut items = list.items.as_mut().unwrap();
        for mut existing_item in items.into_iter() {
            if existing_item.rel.liid == list_item.rel.liid {
                existing_item.title = list_item.title.clone();
                existing_item.description = list_item.description.clone();
                is_existing_item = true;
                break;
            }
        }

        // create a new item if needed
        if !is_existing_item {
            // create t_list_item in PG for rel field
            let new_rel_item_template =
                structures_pg::t_list_item::new(list_item.rel.liid, list_item.rel.parent_lid);
            let new_rel_item = structures_pg::structures_pg_impl::put_t_list_item(
                &new_rel_item_template,
                &pg_client,
            )
            .await;
            if new_rel_item.is_none() {
                error!(
                    "Failed to create a new t_list_item for liid: {}, lid: {} ",
                    new_rel_item_template.liid, new_rel_item_template.parent_lid
                );
                return Err("Failed to save this new list item in the DB.".to_string());
            }

            // assign t_list_item to rel field
            let new_ddb_item = LdListItem {
                title: list_item.title.clone(),
                description: list_item.description.clone(),
                rel: new_rel_item.unwrap(),
            };

            items.push(new_ddb_item);
        }

        // update the list in the DB
        let list_updated = list.save_in_ddb(&ddb_client, &pg_client).await;

        if list_updated.is_err() {
            return Err(list_updated.unwrap_err());
        };

        // extract and return the item as it is in the DB
        let list_updated = list_updated.unwrap();
        if let Some(items) = list_updated.unwrap().items {
            // try to find the matching item in the updated list to return back
            for item_updated in items.into_iter() {
                if item_updated.rel.liid == list_item.rel.liid {
                    return Ok(item_updated);
                }
            }
        };

        // something went wrong - the items we saved is not there
        Err(ERR_MSG_SAVING_ITEM_FAILED.to_string())
    }
}
