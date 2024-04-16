from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

# specify a database
acv7qc = client.acv7qc

# specify a collection
data_project_2 = acv7qc.data_project_2

# specify path
path = "data"

# Imported document count
import_count = 0

# Can't be imported count from json.decoder.JSONDecodeError
no_import_count = 0

data_project_2.delete_many({})

for (root, dirs, file) in os.walk(path):
    for f in file:
        with open(f"{path}/{f}") as new_json:
            try:
                file_data = json.load(new_json)
                if isinstance(file_data, list):
                    data_project_2.insert_many(file_data)  
                else:
                    data_project_2.insert_one(file_data)
                import_count += len(file_data)
            except json.decoder.JSONDecodeError:
                no_import_count += 1
        print(f)

colls = acv7qc.list_collection_names()
print(colls)

count = data_project_2.count_documents({})
print(count)