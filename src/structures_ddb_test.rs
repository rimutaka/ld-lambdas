// Use cargo test -- --nocapture to get the full logging output
#[cfg(test)]
mod tests_ddb {
    use crate::structures_ddb::*;
    use crate::structures_pg::*;
    use log::{self, debug};
    use uuid::Uuid;

    #[tokio::test]
    async fn test_dynamodb_list_listitems_new_update_delete() {
        debug!("test_dynamodb_functions started");

        // prepare some constants
        let user_id = uuid::Uuid::parse_str("dbc44eaa-364f-4a4f-b25e-15218c7928a7").unwrap();
        let lid = Uuid::new_v4();
        let list_title = "My test list X".to_string();

        // prepare DDB and PG connections
        let (pg_client, ddb_client) = test_helpers::init_db_clients().await;

        // create a brand new list template
        let ddb_list_template = LdList::new(lid.clone(), list_title, user_id);

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
        let list_item_from_ui = LdListItem {
            title: "New item 1".to_string(),
            description: Some("Some long description 1".to_string()),
            rel: TListItem::new(liid_1.clone(), lid.clone()),
        };
        let list_item_1 = LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client).await;

        // check if the 1st item was added successfully
        assert!(list_item_1.is_ok());
        let list_item_1 = list_item_1.unwrap();
        assert!(list_item_1.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_1.rel.liid, liid_1); // checks if it's the right item

        // add a 2nd item to the list
        let liid_2 = Uuid::new_v4();
        let list_item_from_ui = LdListItem {
            title: "New item 2".to_string(),
            description: Some("Some long description 2".to_string()),
            rel: TListItem::new(liid_2.clone(), lid.clone()),
        };
        let list_item_2 = LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client).await;

        // check if the 2nd item was added successfully
        assert!(list_item_2.is_ok());
        let list_item_2 = list_item_2.unwrap();
        assert!(list_item_2.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_2.rel.liid, liid_2); // checks if it's the right item

        // modify the 1st item
        let list_item_from_ui = LdListItem {
            title: "New item 1 - still".to_string(),
            description: Some("Some long description - modified".to_string()),
            rel: TListItem::new(liid_1.clone(), lid.clone()),
        };
        let list_item_1a = LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client).await;

        // check if the 1st item was modified successfully
        assert!(list_item_1a.is_ok());
        let list_item_1a = list_item_1a.unwrap();
        assert!(list_item_1a.rel.created_on_utc.is_some()); // checks if the item was created in PG
        assert_eq!(list_item_1a.rel.liid, liid_1); // checks if it's the right item
        assert_ne!(list_item_1a.title, list_item_1.title); // checks if the title changed
        assert_ne!(list_item_1a.description, list_item_1.description); // checks if the description changed

        // delete items one by one
        let list_del_1 = LdListItem::del_list_item_ddb(lid, liid_1.clone(), &ddb_client, &pg_client).await;

        // check if the 1st item was deleted successfully
        for item_remaining in list_del_1.unwrap().unwrap().items.unwrap() {
            assert_ne!(item_remaining.rel.liid, liid_1);
        }
    }

    #[tokio::test]
    async fn test_dynamodb_get_user_lists() {
        debug!("test_dynamodb_get_user_lists started");

        // prepare DDB and PG connections
        let (pg_client, ddb_client) = test_helpers::init_db_clients().await;

        // create a new user
        let user_email = [
            "test_dynamodb_get_user_lists@",
            Uuid::new_v4().to_string().as_str(),
            ".com",
        ]
        .concat();
        let pg_user = put_t_user(&user_email, &pg_client)
            .await
            .expect("Failed to create a new user");

        // create user lists
        let lids: [Uuid; 3] = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let list1 =
            test_helpers::create_random_list(lids[0].clone(), pg_user.user_id.clone(), &ddb_client, &pg_client).await;
        let list2 =
            test_helpers::create_random_list(lids[1].clone(), pg_user.user_id.clone(), &ddb_client, &pg_client).await;
        let list3 =
            test_helpers::create_random_list(lids[2].clone(), pg_user.user_id.clone(), &ddb_client, &pg_client).await;

        // get all user lists using different params
        let all_lists_id_and_email =
            LdList::get_all_user_lists_from_ddb(pg_user.user_id.clone(), &ddb_client, &pg_client).await;
        assert_eq!(
            all_lists_id_and_email
                .expect("all_lists_id_and_email failed")
                .unwrap()
                .len(),
            3
        );

        let all_lists_id_only =
            LdList::get_all_user_lists_from_ddb(pg_user.user_id.clone(), &ddb_client, &pg_client).await;
        assert_eq!(all_lists_id_only.expect("all_lists_id_only failed").unwrap().len(), 3);

        // none of the following tests should return anything

        let all_lists_wrong_id = LdList::get_all_user_lists_from_ddb(Uuid::new_v4(), &ddb_client, &pg_client).await;
        assert!(all_lists_wrong_id.expect("all_lists_wrong_id failed").is_none());

        // clean up
        assert!(del_t_user(pg_user.user_id.clone(), &pg_client).await.is_ok());
        assert!(list1.delete_from_all_dbs(&ddb_client, &pg_client).await.is_ok());
        assert!(list2.delete_from_all_dbs(&ddb_client, &pg_client).await.is_ok());
        assert!(list3.delete_from_all_dbs(&ddb_client, &pg_client).await.is_ok());
    }

    #[tokio::test]
    async fn test_dynamodb_del_user() {
        debug!("test_dynamodb_del_user started");

        // prepare DDB and PG connections
        let (pg_client, _) = test_helpers::init_db_clients().await;

        // create a new user
        let user_email = "test_dynamodb_del_user@example.com".to_string();
        let pg_user = put_t_user(&user_email, &pg_client)
            .await
            .expect("Failed to create a new user");

        // check the user was created
        let pg_user_read = get_t_user(None, Some(user_email.clone()), &pg_client).await;
        assert!(pg_user_read.is_some());

        // delete the user
        assert!(del_t_user(pg_user.user_id.clone(), &pg_client).await.is_ok());

        // check the user was deleted
        let pg_user_read = get_t_user(None, Some(user_email.clone()), &pg_client).await;
        assert!(pg_user_read.is_none());
    }

    #[cfg(test)]
    mod test_helpers {
        use crate::structures_ddb::*;
        use crate::structures_pg::*;
        use crate::utils;
        use log::{self, debug};
        use rand::{self, Rng};

        /// A helper function to create a list with a few list items.
        /// Returns a list of LIID's
        pub(crate) async fn create_random_list(
            lid: Uuid,
            user_id: Uuid,
            ddb_client: &DynamoDbClient,
            pg_client: &tokio_postgres::Client,
        ) -> LdList {
            debug!("create_random_list started");

            // create a brand new list template
            let ddb_list_template = LdList {
                lid: lid.clone(),
                title: ["TEST ", chrono::Utc::now().to_rfc3339().as_str()].concat(),
                description: Some(generate_random_string(25)),
                tags: Some(vec![
                    generate_random_string(1),
                    generate_random_string(1),
                    generate_random_string(1),
                ]),
                items: None,
                rel: TList::new(lid.clone(), user_id.clone()),
            };

            // save it in DDB and PG
            ddb_list_template
                .save_in_ddb(&ddb_client, &pg_client)
                .await
                .expect("Cannot save new LDList");

            // generate list items
            for i in 0..5usize {
                let list_item_from_ui = LdListItem {
                    title: [i.to_string().as_str(), ": ", generate_random_string(15).as_str()].concat(),
                    description: Some(generate_random_string(15)),
                    rel: TListItem::new(Uuid::new_v4(), lid.clone()),
                };
                LdListItem::put_list_item_ddb(list_item_from_ui, &ddb_client, &pg_client)
                    .await
                    .expect("Cannot save new LDListItem");
            }

            // return the resulting list from DDB
            LdList::get_from_ddb(&lid, &ddb_client).await.unwrap().unwrap()
        }

        /// Creates Postgres and DynamoDB connection clients in one sweep.
        pub(crate) async fn init_db_clients() -> (tokio_postgres::Client, rusoto_dynamodb::DynamoDbClient) {
            simple_logger::init_with_level(log::Level::Debug).expect("Cannot initialise simple_logger");
            debug!("init_db_clients started");

            // prepare DDB and PG connections
            let ddb_client = rusoto_dynamodb::DynamoDbClient::new(rusoto_core::Region::UsEast1);
            debug!("ddb_client created");
            let pg_client = utils::get_pg_client().await;

            return (pg_client, ddb_client);
        }

        /// Generates a random string that looks like a sentence.
        pub fn generate_random_string(word_count: usize) -> String {
            let mut rng = rand::thread_rng();
            let mut str_builder = String::new();

            // generate one word per cycle
            for _i in 0..word_count {
                // generate a word of random length
                let s: String = rand::thread_rng()
                    .sample_iter(&rand::distributions::Alphanumeric)
                    .take(rng.gen_range(2, 15))
                    .collect();

                // add it to the output + a space
                str_builder.push_str(&s);
                str_builder.push(' ');
            }

            // truncate the last space and return
            str_builder.trim_end().to_string()
        }
    }
}
