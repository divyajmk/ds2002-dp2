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
collection = "dataproject2files"

# Switch to UVA computing ID database
db = client[database]

# Create a new collection
new_collection = db[collection]

all_files = []
# List Directory Contents
# Iterate through each file in the folder
for f in os.listdir("data"):
    file = os.path.join("data",f)
    # Now open and read each JSON files
    with open(file, 'r') as file:
        files = json.load(file)
        all_files.append(files)

print(all_files)