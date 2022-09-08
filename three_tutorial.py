try:
    import os
    import datetime
    import json

    from pymongo import MongoClient
    from dotenv import load_dotenv, find_dotenv
    from pprint import pprint as p
except ModuleNotFoundError as err:
    print("[-] Failed to import modules")
    print(err)

load_dotenv(find_dotenv(), verbose=True)
password = os.environ.get("MONGODB_PWD")
connection_string = f"mongodb+srv://ranjan-viper:{password}@sandbox-1.abmfe0d.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_string)

# ---------------------------------------------------------------------
# Reading JSON data


def load_json(data_path: str) -> list:
    file = open(data_path)
    data = json.load(file)
    return data

# JSON_DATA_PATH = "data/JEOPARDY_QUESTIONS.json"
# data_for_db = load_json(JSON_DATA_PATH)

# --------------------------------------------------------------------
# Insert data in database


def insert_data_in_db(data: list, collection_name: str, db_name: str) -> None:
    try:
        client[db_name][collection_name].insert_many(data)
        print("[+] Data insert successful")
    except Exception as err:
        print(f"[-] {err}")

# insert_data_in_db(data_for_db, "question", "Jeopardy_db")

# ---------------------------------------------------------------------
# Fuzzy matching


question = client.Jeopardy_db.question


def fuzzy_matching(query_string):
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": query_string,
                    "path": "category",
                    "fuzzy": {}
                }
            }
        },
        {
            "$limit": 10
        }
    ])

    return list(result)

# p(fuzzy_matching("computer"))

# ---------------------------------------------------------------------
# Synonym matching


def synonym_matching(query_string: str):
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "text": {
                    "query": query_string,
                    "path": "category",
                    "synonyms": "mapping"
                }
            }
        },
        {
            "$limit": 10
        }
    ])

    return list(result)

# p(synonym_matching("anger"))

# ---------------------------------------------------------------------
# Auto complete


def auto_complete(query_string: str):
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "autocomplete": {
                    "query": query_string,
                    "path": "question",
                    "tokenOrder": "sequential",
                    "fuzzy": {}
                }
            }
        }, {
            "$project": {
                "question": 1,
                "_id": 0
            }
        },
        {
            "$limit": 10
        }
    ])

    return list(result)

# p(auto_complete("Insane life"))

# ---------------------------------------------------------------------
# Compound queries


def compound_queries(must, must_not , should):
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": must,
                                "path": "category"
                            }
                        }
                    ],
                    "mustNot": [
                        {
                            "text": {
                                "query": must_not,
                                "path": "category"

                            }
                        }
                    ],
                    "should": [
                        {
                            "text": {
                                "query": should,
                                "path": "answer"
                            }
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "_id": 0,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$limit": 10
        }
    ])

    return list(result)

# p(compound_queries(["Computer", "Coding"], "codes", "application"))

# ---------------------------------------------------------------------
# Relevancy search


def relevance_queries(must, should1, should2):
    result = question.aggregate([
        {
            "$search": {
                "index": "language_search",
                "compound": {
                    "must": [
                        {
                            "text": {
                                "query": must,
                                "path": "category"
                            }
                        }
                    ],
                    "should": [
                        {
                            "text": {
                                "query": should1,
                                "path": "round",
                                "score": {"boost": {"value": 3.0}}
                            }
                        },
                        {
                            "text": {
                                "query": should2,
                                "path": "round",
                                "score": {"boost": {"value": 2.0}}
                            }
                        }
                    ]
                }
            }
        },
        {
            "$project": {
                "question": 1,
                "answer": 1,
                "category": 1,
                "_id": 0,
                "score": {"$meta": "searchScore"}
            }
        },
        {
            "$limit": 10
        }
    ])

    return list(result)

p(relevance_queries("geography", "Final Jeopardy", "Double Jeopardy"))