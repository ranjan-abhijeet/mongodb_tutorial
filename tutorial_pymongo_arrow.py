try:
    import os
    import pprint
    import pyarrow
    import pymongoarrow

    from pymongoarrow.api import Schema
    from pymongoarrow.monkey import patch_all
    from bson.objectid import ObjectId
    from pymongo import MongoClient
    from dotenv import load_dotenv, find_dotenv
    from datetime import datetime
except ModuleNotFoundError as err:
    print("[-] Failed to import modules")
    print(err)

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://ranjan-viper:{password}@sandbox-1.abmfe0d.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)


patch_all()

author_schema = Schema({
    "_id": ObjectId,
    "first_name": pyarrow.string(), 
    "last_name": pyarrow.string(),
    "date_of_birth": datetime
    })

arrow_table = client.Library.author.find_arrow_all({}, schema=author_schema)
print(arrow_table)

df = client.Library.author.find_pandas_all({}, schema=author_schema)
print(df.head())

ndarrays = client.Library.author.find_numpy_all({}, schema=author_schema)
print(df.head())



