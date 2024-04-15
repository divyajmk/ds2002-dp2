#!/home/gitpod/.pyenv/shims/python3


from pymongo import MongoClient, errors
from bson.json_util import dumps
import os
import json

# MongoDB connection
MONGOPASS = os.getenv('MONGOPASS')
uri = "mongodb+srv://cluster0.pnxzwgz.mongodb.net/"
client = MongoClient(uri, username='nmagee', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)

database = "yaq7fm"
collection_name = "dataproject2files"

# Switch to UVA computing ID database
db = client[database]

# Create a new collection
collection = db[collection_name]

total_docs = 0
successful_imports = 0
failed_imports = 0
corrupted_files = 0
# List Directory Contents
# Iterate through each file in the folder
for f in os.listdir("data"):
    file_path = os.path.join("data",f)
    # Now open and read each JSON files
    with open(file_path, 'r') as file:
        try:
            # Try reading in json files 
            files = json.load(file)
            # Importing JSON file into MongoDB
            # Insert loaded data into collection
            if isinstance(files, list):
                collection.insert_many(files)
                total_docs += len(files)
            else:
                collection.insert_one(files)
                total_docs += 1
        except json.JSONDecodeError as e:
            # Error handling for when loading JSON file
            corrupted_files += 1
        except Exception as e:
            if isinstance(files,list):
                failed_imports += len(files)
            else:
                failed_imports += 1

print("Total documents imported:", total_docs)
print("Failed imports:", failed_imports)
print("Corrupted documents:", corrupted_files)

