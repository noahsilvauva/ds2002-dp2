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

# Corrupted count from json.decoder.JSONDecodeError
corrupt_count = 0

# Complete but not imported because of json.decoder.JSONDecodeError
complete_not_imported_count = 0

# Delete any files imported into the database from running the script earlier
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
                corrupt_count += 1
                complete_not_imported_count += len(file_data) - 1
                continue

print("Records imported: ", import_count)
print("Records orphaned: ", complete_not_imported_count)
print("Records corrupted: ", corrupt_count)






































































































































































































































































































































































# Sources:

# https://www.geeksforgeeks.org/formatted-string-literals-f-strings-python/
# https://docs.python.org/3/tutorial/errors.html
# https://www.w3schools.com/python/python_file_write.asp