from pymongo import MongoClient

import config


def sync_mongodb(from_url, to_url):
    """
    Sync all local MongoDB databases with remote ones.
    :param from_url: Connection URI for source MongoDB
    :param to_url: Connection URI for destination MongoDB
    """
    from_client = MongoClient(from_url)
    to_client = MongoClient(to_url)

    for db_name in from_client.list_database_names():
        if db_name in ["admin", "config", "local"]:
            continue  # Skip system databases

        from_db = from_client[db_name]
        to_db = to_client[db_name]

        for collection_name in from_db.list_collection_names():
            from_collection = from_db[collection_name]
            to_collection = to_db[collection_name]

            # Clear remote collection before syncing
            to_collection.delete_many({})

            # Insert all documents from local to remote
            documents = list(from_collection.find())
            if documents:
                for doc in documents:
                    doc.pop('_id', None)  # Remove _id to avoid duplication issues
                to_collection.insert_many(documents)

            print(f"Synced collection: {collection_name} ({len(documents)} documents) in database: {db_name}")

    print("Sync completed successfully.")


if __name__ == "__main__":
    local_mongo_uri = config.local_mongo_url
    remote_mongo_uri = config.mongo_url

    sync_mongodb(local_mongo_uri, remote_mongo_uri)