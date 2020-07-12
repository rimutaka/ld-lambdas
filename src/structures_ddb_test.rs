// Use cargo test -- --nocapture to get the full logging output
#[cfg(test)]
mod tests_ddb {
    use crate::structures_pg;
    use crate::structures_ddb;
    use crate::utils;
    use log::{self, debug};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_dynamodb_functions() {
        simple_logger::init_with_level(log::Level::Debug).expect("Cannot initialise simple_logger");
        debug!("main started");
        debug!("");
    
        // prepare some constants
        let user_id = uuid::Uuid::parse_str("dbc44eaa-364f-4a4f-b25e-15218c7928a7").unwrap();
        let lid = Uuid::new_v4();
        let list_title = "My test list X".to_string();
    
        // prepare DDB and PG connections
        let ddb_client = rusoto_dynamodb::DynamoDbClient::new(rusoto_core::Region::UsEast1);
        let pg_client = utils::get_pg_client().await;
        debug!("ddb_client created");
    
        // create a brand new list template
        let ddb_list_template = structures_ddb::LdList::new(lid.clone(), list_title, user_id);
    
        // save it in DDB and PG
        let ddb_list_saved = ddb_list_template.save_in_ddb(&ddb_client, &pg_client).await;
    
        // check if saved successfully
        assert!(ddb_list_saved.is_ok());
        let ddb_list_saved = ddb_list_saved.unwrap();
        assert!(ddb_list_saved.is_some());
    
        // update the list - add description
        let mut list_to_update = ddb_list_saved.unwrap();
        let new_descr = "Updated description".to_string();
        list_to_update.description = Some(new_descr.clone());
        let list_updated = list_to_update.save_in_ddb(&ddb_client, &pg_client).await;
    
        // check if updated successfully
        assert!(list_updated.is_ok());
        let list_updated = list_updated.unwrap();
        assert!(list_updated.is_some());
        let list_updated = list_updated.unwrap();
        assert!(list_updated.description.is_some());
        assert_eq!(list_updated.description.unwrap(), new_descr);
    
        // add the 1st item to the list
        let liid_1 = Uuid::new_v4();
        let list_item_from_ui = structures_ddb::LdListItem {
            title: "New item 1".to_string(),
            description: Some("Some long description 1".to_string()),
            rel: structures_pg::TListItem::new(liid_1.clone(), lid.clone()),
        };
        let list_item_1 =
            structures_ddb::LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client)
                .await;
    
        // check if the 1st item was added successfully
        assert!(list_item_1.is_ok());
        let list_item_1 = list_item_1.unwrap();
        assert!(list_item_1.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_1.rel.liid, liid_1); // checks if it's the right item
    
        // add a 2nd item to the list
        let liid_2 = Uuid::new_v4();
        let list_item_from_ui = structures_ddb::LdListItem {
            title: "New item 2".to_string(),
            description: Some("Some long description 2".to_string()),
            rel: structures_pg::TListItem::new(liid_2.clone(), lid.clone()),
        };
        let list_item_2 =
            structures_ddb::LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client)
                .await;
    
        // check if the 2nd item was added successfully
        assert!(list_item_2.is_ok());
        let list_item_2 = list_item_2.unwrap();
        assert!(list_item_2.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_2.rel.liid, liid_2); // checks if it's the right item
    
        // modify the 1st item
        let list_item_from_ui = structures_ddb::LdListItem {
            title: "New item 1 - still".to_string(),
            description: Some("Some long description - modified".to_string()),
            rel: structures_pg::TListItem::new(liid_1.clone(), lid.clone()),
        };
        let list_item_1a =
            structures_ddb::LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client)
                .await;
    
        // check if the 1st item was modified successfully
        assert!(list_item_1a.is_ok());
        let list_item_1a = list_item_1a.unwrap();
        assert!(list_item_1a.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_1a.rel.liid, liid_1); // checks if it's the right item
        assert_ne!(list_item_1a.title, list_item_1.title); // checks if the title changed
        assert_ne!(list_item_1a.description, list_item_1.description); // checks if the description changed
    
        // delete items one by one
        let list_del_1 =
            structures_ddb::LdListItem::del_list_item_ddb(lid, liid_1.clone(), &ddb_client, &pg_client)
                .await;
    
        // check if the 1st item was deleted successfully
        for item_remaining in list_del_1.unwrap().unwrap().items.unwrap() {
            assert_ne!(item_remaining.rel.liid, liid_1);
        }



    }
}
