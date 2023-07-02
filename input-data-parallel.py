import os
import sys
from random import randint

import pymongo
from dotenv import load_dotenv
from faker import Faker
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

load_dotenv()
CONNECTION_STRING = os.environ.get("COSMOS_CONNECTION_STRING")
# </client_credentials>

# <constant_values>
DB_NAME = "adventureworks"
COLLECTION_NAME = "products"
# </constant_values>

i = 0
num_items = 1000000

def create_and_insert_product(collection, fake, i):
    if i > num_items:
        return
    product = {
        "category": fake.word(),
        "job": fake.job(),
        "city": fake.city(),
        "name": "{} {}-{}".format(fake.word(), fake.word(), randint(50, 5000)),
        "quantity": 1,
        "sale": False,
    }

    wait_num = 0

    try:
        result = collection.update_one(
            {"name": product["name"]}, {"$set": product}, upsert=True
        )
        i+=1
        if i % 10000 == 0:
            now = datetime.now()
            formatted_now = now.strftime("%Y-%m-%d %H:%M:%S")
            print("Upserted {} documents at {}, error case {}\n".format(i, formatted_now, wait_num))
                
    except pymongo.errors.WriteError as err:
        #print(err)
        wait_num += 1
        time.sleep(1)

    return wait_num

def main():
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
    
    with ThreadPoolExecutor(max_workers=24) as executor:
        fake = Faker()
        start_time = time.time()
        wait_nums = list(executor.map(create_and_insert_product, [collection]*num_items, [fake]*num_items, range(num_items)))
        end_time = time.time()
        elapsed_time = end_time - start_time
        print("{} items insert completed, elapsed time {} sec. wait occured {} times.".format(num_items, elapsed_time, sum(wait_nums)))

if __name__ == "__main__":
    main()