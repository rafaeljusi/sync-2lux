import ast
import json
import math
import time
import numpy as np
import pandas as pd
from bson.codec_options import CodecOptions, DEFAULT_CODEC_OPTIONS
from bson.binary import Binary, UuidRepresentation
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

def getDBClient():
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

def getProducts():
    with open('config_products.json') as config_file:
        config = json.load(config_file)

    dataset=pd.read_csv(config["source_file"], encoding= 'unicode_escape', engine='python', header=0, names=config["headers"], dtype=config["dtypes"], parse_dates=config["parse_dates"])

    dataset = dataset.replace({np.nan: None})
    dataset['brand'] = dataset['brand'].apply(lambda x: x.upper() if isinstance(x, str) else None);
    dataset['image'] = dataset['image'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else None);
    dataset['product_category_tree'] = dataset['product_category_tree'].apply(lambda x: x.strip('[]').strip('""').split(' >> ') if isinstance(x, str) else None);
    dataset['product_specifications'] = dataset['product_specifications'].apply(lambda x: json.loads(x.replace("=>", ":")).get("product_specification") if isinstance(x, str) and is_json(x.replace("=>", ":")) else None);

    return dataset

def writeRecordsToDB(dataset, collection):
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

def main():
    db = getDBClient()

    start = time.time()

    print("Reading products from file...")
    products = getProducts()

    print("Writing products to DB...")
    collection= db.get_collection("products", CODEC_OPTIONS)
    writeRecordsToDB(dataset=products, collection=collection)

    end = time.time()
    timeElapsed = end - start
    recordCount = len(products)

    print(f'Finished. Records: {recordCount}. Time elapsed: {timeElapsed}. ~{recordCount / timeElapsed:.0f} records per second')
