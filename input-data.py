# -----------------------------------------------------------------------------
#  Prerequisites:
#
# 1. An Azure Cosmos DB API for MongoDB Account.
# 2. PyMongo installed.
# 3. python-dotenv installed (to load environment variables from a .env file).
# -----------------------------------------------------------------------------
# Sample - shows doc CRUD operations oin the Azure Cosmos DB API for MongoDB
#        - for use in quickstart article
# -----------------------------------------------------------------------------

# <package_dependencies>
import os
import sys
from random import randint

import pymongo
from dotenv import load_dotenv
from faker import Faker
import time
from datetime import datetime
# </package_dependencies>

# <client_credentials>
load_dotenv()
CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
# </client_credentials>

# <constant_values>
DB_NAME = "adventureworks"
COLLECTION_NAME = "products"
# </constant_values>


def main():
    """Connect to the API for MongoDB, create DB and collection,
    perform CRUD operations
    """

    try:
        # <connect_client>
        client = pymongo.MongoClient(CONNECTION_STRING)
        # </connect_client>
        try:
            client.server_info()  # validate connection string
        except (
            pymongo.errors.OperationFailure,
            pymongo.errors.ConnectionFailure,
            pymongo.errors.ExecutionTimeout,
        ) as err:
            sys.exit("Can't connect:" + str(err))
    except Exception as err:
        sys.exit("Error:" + str(err))

    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # <new_doc>
    """Create new document and upsert (create or replace) to collection"""
    num_items = 1000000
    fake = Faker()
    start_time = time.time()
    wait_num = 0

    for i in range(num_items):
        product = {
            "category": fake.word(),
            "job": fake.job(),
            "city": fake.city(),
            "name": "{} {}-{}".format(fake.word(), fake.word(), randint(50, 5000)),
            "quantity": 1,
            "sale": False,
        }

        try:
            result = collection.update_one(
                {"name": product["name"]}, {"$set": product}, upsert=True
            )
            if i % 10000 == 0:
                now = datetime.now()
                formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
                print("Upserted {} documents at {}, error case {}\n".format(i, formatted_now, wait_num))
                
        except pymongo.errors.WriteError as err:
            #print(err)
            wait_num += 1
            time.sleep(1)
        # print("Upserted document with _id {}\n".format(result.upserted_id))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print("{} items insert completed, elapsed time {} sec. wait occured {} times.".format(num_items, elapsed_time, wait_num))

if __name__ == "__main__":
    main()
