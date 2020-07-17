// Use cargo test -- --nocapture to get the full logging output
#[cfg(test)]
mod tests_pg {
    use crate::structures_pg::*;
    use crate::utils::get_pg_client;
    use log::{self, debug};
    use uuid::Uuid;
    use crate::utils;

    #[tokio::test]
    async fn test_postgres_functions() {
        utils::log_init(log::Level::Debug);
        debug!("main started");

        // Connect to the database.
        let client = get_pg_client().await;

        // create a new user
        let user_email = ["test_postgres_functions@", Uuid::new_v4().to_string().as_str(), ".com"].concat();
        let pg_user = put_t_user(&user_email, &client).await;
        assert!(pg_user.is_some());

        // get the user with diff input combos
        let pg_user = pg_user.unwrap(); // it's safe to unwrap after the assert! for Some().
        let pg_user_g1 = get_t_user(None, Some(pg_user.user_email.clone()), &client).await;
        let pg_user_g2 = get_t_user(Some(pg_user.user_id.clone()), None, &client).await;
        let pg_user_g3 = get_t_user(Some(pg_user.user_id.clone()), Some(pg_user.user_email.clone()), &client).await;
        let pg_user_g4 = get_t_user(None, None, &client).await;
        let pg_user_g5 = get_t_user(Some(Uuid::new_v4()), Some(pg_user.user_email.clone()), &client).await;
        assert!(pg_user_g1.is_some());
        assert!(pg_user_g2.is_some());
        assert!(pg_user_g3.is_some());
        assert!(pg_user_g4.is_none());
        assert!(pg_user_g5.is_none());

        // create a new list
        let pg_list = TList::new(Uuid::new_v4(), pg_user.user_id.clone());
        let list_put = put_t_list(&pg_list, &client).await;
        debug!("list created: {:?}", list_put);

        // create new items
        let pg_list_item_1 = TListItem::new(Uuid::new_v4(), pg_list.lid);
        let pg_list_item_2 = TListItem::new(Uuid::new_v4(), pg_list.lid);
        let pg_list_item_3 = TListItem::new(Uuid::new_v4(), pg_list.lid);
        debug!("dummy item created: {:?}", pg_list_item_1);

        let pg_list_item_1p = put_t_list_item(&pg_list_item_1, &client).await;
        let pg_list_item_2p = put_t_list_item(&pg_list_item_2, &client).await;
        let pg_list_item_3p = put_t_list_item(&pg_list_item_3, &client).await;
        debug!("pg item created: {:?}", pg_list_item_1p);

        // get single item
        let pg_list_item_1g = get_t_list_item(pg_list_item_1p.clone().unwrap().liid, &client).await;
        debug!("pg item retrieved: {:?}", pg_list_item_1g);

        // get all items for the list
        let items_get = get_t_list_items(pg_list.lid, &client).await;
        debug!(
            "pg items retrieved: {}",
            items_get.as_ref().map(|itg| itg.len()).unwrap_or_else(|| 0)
        );

        // get a single list
        let list_get = get_t_list(pg_list.lid, &client).await;
        debug!("pg list retrieved: {:?}", list_get);

        // assert
        assert!(pg_list_item_1p.is_some());
        assert!(pg_list_item_2p.is_some());
        assert!(pg_list_item_3p.is_some());
        assert_eq!(pg_list_item_1p, pg_list_item_1g);
        let p1 = pg_list_item_1p.unwrap();
        let p2 = pg_list_item_2p.unwrap();
        let p3 = pg_list_item_3p.unwrap();
        assert_eq!(items_get, Some(vec!(p1, p2, p3)));

        // test deletion of a single item
        del_t_list_item(pg_list_item_1.liid, &client).await;
        let pg_list_item_1d = get_t_list_item(pg_list_item_1.liid, &client).await;
        let pg_list_item_2d = get_t_list_item(pg_list_item_2.liid, &client).await;
        assert!(pg_list_item_1d.is_none());
        assert!(pg_list_item_2d.is_some());

        // delete the list and all the other items with it - there should be none left
        del_t_list(pg_list_item_1.parent_lid, &client).await.expect("del_t_list failed");
        let pg_list_item_2d = get_t_list_item(pg_list_item_2.liid, &client).await;
        let pg_list_item_3d = get_t_list_item(pg_list_item_3.liid, &client).await;
        let list_d = get_t_list(pg_list_item_1.parent_lid, &client).await;
        assert!(pg_list_item_2d.is_none());
        assert!(pg_list_item_3d.is_none());
        assert!(list_d.is_none());

        // delete the user
        del_t_user(pg_user.user_id.clone(), &client).await.expect("del_t_list failed");
        let pg_user_d1 = get_t_user(Some(pg_user.user_id.clone()), None, &client).await;
        assert!(pg_user_d1.is_none());
    }
}
