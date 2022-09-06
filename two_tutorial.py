try:
    import os
    import pprint
    import datetime

    from mdb_operator import get_operator
    from pymongo import MongoClient
    from dotenv import load_dotenv, find_dotenv
except ModuleNotFoundError as err:
    print("[-] Failed to import modules")
    print(err)

load_dotenv(find_dotenv())
password = os.environ.get("MONGODB_PWD")

connection_string = f"mongodb+srv://ranjan-viper:{password}@sandbox-1.abmfe0d.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)


def check_db(db_name: str):
    if db_name in client.list_database_names():
        return True
    return False


def check_collection(collection_name: str, db_name: str):
    if check_db(db_name):
        if collection_name in client[db_name].list_collection_names():
            return True
    return False


def create_db(db_name: str):
    if check_db(db_name):
        new_db = client[db_name]
        collection = new_db.create_collection("initialisation")
        initial_document = {
            "name": "initialisation document",
            "date": datetime.datetime.now()
        }
        collection.insert_one(initial_document)
        print(f"[+] Database '{db_name}' initialised")
        return client[db_name]
    else:
        print(f"[-] Database '{db_name}' already exists")
        return None


"""
create_db(db_name="User")
"""


def create_collection(collection_name: str, db_name: str):
    if check_db(db_name):
        if not check_collection(collection_name, db_name):
            collection = client[db_name].create_collection(collection_name)
            initial_document = {
                "name": "initialisation document",
                "date": datetime.datetime.now()
            }
            collection.insert_one(initial_document)
            print(
                f"[+] Collection '{collection_name}' initialised in database '{db_name}'")
            return client[db_name][collection_name]
        else:
            print(
                f"[-] Collection '{collection_name}' already exists in database '{db_name}'")
            return None
    else:
        print(f"[-] Database '{db_name}' does not exist.")
        return None


"""
create_collection(collection_name="employee", db_name="User")
"""


def insert_one_document(document: dict, collection_name: str = "employee", db_name: str = "User"):
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            pass
