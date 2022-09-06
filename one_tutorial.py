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
            insert_id = client[db_name][collection_name].insert_one(
                document).inserted_id
            return insert_id
        else:
            print(
                f"[-] Collection '{collection_name}' does not exist in database '{db_name}'")
            return None
    else:
        print(f"[-] Database '{db_name}' does not exist.")
        return None


my_document = {"first_name": "Abhijeet",
               "last_name": "Ranjan",
               "age": 28,
               "company": "Kaatru, IIT Madras"}

"""

insert_one_document(document=my_document,
                    collection_name="employee",
                    db_name="User")
"""


def insert_many_documents(document_list: list, collection_name: str = "employee", db_name: str = "User"):
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            insert_ids = client[db_name][collection_name].insert_many(
                document_list).inserted_ids
            return insert_ids
        else:
            print(
                f"[-] Collection '{collection_name}' does not exist in database '{db_name}'")
            return None
    else:
        print(f"[-] Database '{db_name}' does not exist.")
        return None


"""
first_names = ["Priyanka", "Aviral", "Rishabh", "Varun", "Sargun"]
last_names = ["Bisht", "Seli", "Mathur", "Sethi", "Malik"]
ages = [27, 30, 29, 28, 28]

document_list = [] 

for first_name, last_name, age in zip(first_names, last_names, ages):
    doc = {"first_name": first_name,
            "last_name": last_name,
            "age": age,
            "time": datetime.datetime.now()}
    document_list.append(doc)


insert_many_documents(document_list=document_list,
                    collection_name="employee",
                    db_name="User")
"""


def get_all_documents(collection_name: str = "employee", db_name: str = "User", show: bool = False):
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            documents = client[db_name][collection_name].find()
            if show:
                for document in documents:
                    pprint.pprint(document)
            return list(documents)


"""
get_all_documents(collection_name="employee", db_name="User", show=False)
"""


def find_specific_document(key_pair: dict, collection_name: str = "employee", db_name: str = "User", show: bool = True):
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            documents = client[db_name][collection_name].find(key_pair)
            if show:
                for document in documents:
                    pprint.pprint(document)
            return list(documents)


"""
key_pair = {"first_name": "Abhijeet",
            "age": 28}

find_specific_document(key_pair)    
"""


def count_all_documents(filter_query: dict = {}, collection_name: str = "employee", db_name: str = "User", show: bool = False):
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            count = client[db_name][collection_name].count_documents(
                filter_query)
            if show:
                print(
                    f"[+] Found {count} documents in collection '{collection_name}' of database '{db_name}'")
            return count


"""
filter_query = {"first_name": "Abhijeet"}
count_all_documents(filter_query=filter_query, show=True)
"""


def generate_range_query(query_list: list) -> dict:
    """
    query_list is list of dictiornaries which contain the 
    queries in following format:
    example:
        query_list = [
                    {"feature":"age", "lr": 10, "ur": 20},
                    {"feature":"schooling", "lr": 0, "ur": 35}    
                    ]
    """
    return_list = []
    for query in query_list:
        instance_query = {}
        feature = query["feature"]
        lr = query["lr"]
        ur = query["ur"]
        instance_query[feature] = {get_operator(">="): lr,
                                   get_operator("<="): ur}
        return_list.append(instance_query)

    return {"$and": return_list}


def get_range_query(query: dict,  collection_name: str = "employee", db_name: str = "User", show: bool = False) -> dict:
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            cursor_object = client[db_name][collection_name].find(query)
            if show:
                for object in cursor_object:
                    pprint.pprint(object)

            return list(cursor_object)


"""
query = generate_range_query([{"feature": "age",
                               "lr": 27,
                               "ur": 28}])

get_range_query(query, show=True)
"""


def get_projected_query(query: dict, projected_columns: dict, collection_name: str = "employee", db_name: str = "User", show: bool = False) -> dict:
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            cursor_object = client[db_name][collection_name].find(
                query, projected_columns)
            if show:
                for object in cursor_object:
                    pprint.pprint(object)

            return list(cursor_object)


"""
query = generate_range_query([{"feature": "age",
                               "lr": 27,
                               "ur": 28}])

projected_columns = {"_id": 0, "first_name": 1, "last_name": 1}
get_projected_query(query, projected_columns, show=True)
"""


def update_set_by_id(person_id: str, new_field: dict, collection_name: str = "employee", db_name: str = "User") -> None:
    """
    This function updates information for existing user.
    Tread with caution!

    new_field : {"field": value}
    """
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            from bson import ObjectId

            _id = ObjectId(person_id)

            _filter = {"_id": _id}
            _update = {
                "$set": new_field
            }

            client[db_name][collection_name].update_one(_filter, _update)


"""
update_set_by_id("6316e728c9a6088e71e5101d", {"married": False})    
"""


def delete_unset_by_id(person_id: str, remove_field: str, collection_name: str = "employee", db_name: str = "User") -> None:
    """
    This function removes information from database.
    Tread with caution!
    """
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            from bson import ObjectId

            _id = ObjectId(person_id)

            _filter = {"_id": _id}

            _remove = {
                "$unset": {remove_field: ""}
            }

            client[db_name][collection_name].update_one(_filter, _remove)


"""
delete_unset_by_id("6316e728c9a6088e71e5101d", "married") 
"""


def replace_document_by_id(person_id: str, replacement_doc: dict, collection_name: str = "employee", db_name: str = "User") -> None:
    """
    This function replaces the entire document except the ObjectID.
    Tread with caution!
    """
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            from bson import ObjectId

            _id = ObjectId(person_id)

            _filter = {"_id": _id}

            client[db_name][collection_name].replace_one(filter=_filter, replacement=replacement_doc)

"""
replacement_doc = {
    "first_name": "Abhijeet",
    "last_name": "Ranjan",
    "age": 28,
    "company": "Viper Tech"
}

replace_document_by_id("6316e6f7ccacaea576e8374d", replacement_doc)

"""

def delete_doc_by_id(person_id: str, collection_name: str = "employee", db_name: str = "User") -> None:
    """
    This function deletes the information from database.
    Tread with caution!
    """
    
    if check_db(db_name):
        if check_collection(collection_name, db_name):
            from bson import ObjectId

            _id = ObjectId(person_id)
            
            _filter = {"_id": _id}

            client[db_name][collection_name].delete_one(_filter)


"""
delete_doc_by_id("6316e728c9a6088e71e51019")
"""
