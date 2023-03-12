import ast
from datetime import date, datetime
import json
import os
import time
import numpy as np
import pandas as pd
from bson.codec_options import CodecOptions
from bson.binary import UuidRepresentation
from uuid import UUID
from pymongo import MongoClient
from pymongo import UpdateOne

def is_json(myjson):
  try:
    json.loads(myjson)
  except ValueError as e:
    return False
  return True

CODEC_OPTIONS = CodecOptions(uuid_representation=UuidRepresentation.STANDARD)

def get_dbclient():
    with open('config.json') as config_file:
        config = json.load(config_file)

    try:
        connection_string =config["connection_string"]
        database_name = config["database_name"]
        
        client = MongoClient(connection_string)
        client.server_info()

        db = client[database_name]
        return db
    except:
        print("Database Server not available")
        quit()

def get_products():
    with open('config_products.json') as config_file:
        config = json.load(config_file)

    dataset=pd.read_csv(config["source_file"], encoding= 'unicode_escape', engine='python', header=0, names=config["headers"], dtype=config["dtypes"], parse_dates=config["parse_dates"])

    dataset['product_rating'] = dataset['product_rating'].apply(pd.to_numeric, errors='coerce')
    dataset = dataset.replace({np.nan: None})
    dataset['brand'] = dataset['brand'].apply(lambda x: x.upper() if isinstance(x, str) else None);
    dataset['image'] = dataset['image'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else None);
    dataset['product_category_tree'] = dataset['product_category_tree'].apply(lambda x: x.strip('[]').strip('""').split(' >> ') if isinstance(x, str) else None);
    dataset['product_specifications'] = dataset['product_specifications'].apply(lambda x: json.loads(x.replace("=>", ":")).get("product_specification") if isinstance(x, str) and is_json(x.replace("=>", ":")) else None);

    return dataset

def write_records_to_db(dataset, collection):
    if(len(dataset) > 0):

        data_dict = dataset.to_dict("records")
        operations = []

        for doc in data_dict:
            id = UUID(hex=doc["uniq_id"])
            
            operation = UpdateOne({ "_id": id },{ "$set": doc }, upsert=True)
            operations.append(operation)

            if (len(operations) == 1000):
                collection.bulk_write(operations,ordered=False)
                operations = []

        if (len(operations) > 0):
            collection.bulk_write(operations,ordered=False)

def save_state():
    state = get_state()

    state["last_sync"] = date.today().strftime('%Y-%m-%d')

    with open('state.json', 'w') as state_file:
            state_file.write(json.dumps(state))

def get_state():
    state = {}

    if os.path.exists('state.json') and os.path.getsize('state.json') > 0:
        with open('state.json') as state_file:
            state = json.load(state_file)
            state['last_sync'] = datetime.strptime(state['last_sync'], '%Y-%m-%d').date()

    return state

def main():
    state = get_state()
    
    if(state.get('last_sync') != None and state["last_sync"] >= date.today()):
        print(f'Last sync: {state["last_sync"]}. Sync skipped')
        quit()

    db = get_dbclient()

    start = time.time()

    print("Reading products from file...")
    products = get_products()

    print("Writing products to DB...")
    collection= db.get_collection("products", CODEC_OPTIONS)
    write_records_to_db(dataset=products, collection=collection)

    end = time.time()
    time_elapsed = end - start
    record_count = len(products)

    save_state()

    print(f'Finished => records: {record_count}. time elapsed: {time_elapsed:.0f}s. ~{record_count / time_elapsed:.0f} records per second')
