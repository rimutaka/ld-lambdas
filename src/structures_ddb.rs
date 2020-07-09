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
use uuid::Uuid;

//#[path ="./structures_pg.rs"]
//mod structures_pg;
//#[path ="./structures_pg_impl.rs"]
//mod structures_pg_impl;

// DDB structures


const TABLE_NAME_TLIST: &str = "tlist";
const TABLE_KEY_FOR_TLIST: &str = "lid";

/// A single list item. Part of LdList.
#[derive(Item, Debug, Serialize, Deserialize)]
pub(crate) struct LdListItem {
    #[dynomite(partition_key)]
    pub liid: Uuid,
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
    /// Create a new list with no items
    pub(crate) fn new(title: String, t_list: structures_pg::t_list) -> LdList {
        LdList {
            lid: t_list.lid.clone(),
            title: title,
            description: None,
            tags: None,
            items: None,
            rel: t_list,
        }
    }

    /// Save itself in DDB, get the latest version back and return it in the result.
    pub(crate) async fn save_in_ddb(self, ddb_client: &DynamoDbClient) -> Result<Self, &str> {

        // this var will be used a few times
        let lid = self.lid.clone();

        debug!("save_in_ddb for {}", lid);

        // put the item
        if let Err(put_err) = ddb_client
            .put_item(utils::build_ddb_put_input(self.into(), TABLE_NAME_TLIST))
            .await
        {
            error!("Failed to put_item {:?}", put_err);
            return Err("Failed to save in DDB.");
        }
        debug!("Item put in DDB.");

        // retrieve the latest copy, which may be a bit different from what was saved
        match ddb_client
            .get_item(utils::build_ddb_get_input(TABLE_KEY_FOR_TLIST, &lid, TABLE_NAME_TLIST))
            .await
        {
            Ok(get_item_output) => {
                match get_item_output.item {
                    Some(output_item) => {
                        debug!("Raw from DDB: {:?}", output_item);

                        let new_self = LdList::from_attrs(output_item)
                            .expect("Error converting DDB list into LdList");

                        debug!("Serialized from DDB: {:?}", new_self);
                        return Ok(new_self);
                    }
                    None => {
                        error!("Just-saved DDB item could not be retrieved - no error, no data.");
                        return Err("Failed to save the item. Try again.");
                    }
                };
            }
            Err(error) => {
                error!("DDB error {}", error);
                return Err("Failed to save the item. Try again.");
            }
        }
    }
}
