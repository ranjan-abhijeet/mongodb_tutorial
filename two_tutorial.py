try:
    import os
    import pprint
    import datetime

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


# --------------------------------------------------------------------
"""
Schema validation in MongoDB after creating collection
"""


def create_book_collection():
    book_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["title", "authors", "publish_date", "genre", "copies"],
            "properties": {
                "title": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "authors": {
                    "bsonType": "array",
                    "items": {
                        "bsonType": "objectId",
                        "description": "must be an objectId and is requried"
                    }
                },
                "publish_date": {
                    "bsonType": "date",
                    "description": "must be a date and is required"
                },
                "genre": {
                    "enum": ["Fiction", "Non-Fiction"],
                    "description": "can only be one of the enum values and is required"
                },
                "copies": {
                    "bsonType": "int",
                    "minimum": 0,
                    "description": "must be an integer greater than 0 and is required"
                }
            }
        }
    }

    try:
        client.Library.create_collection("book")
    except Exception as e:
        print(f"[-] {e}")

    client.Library.command("collMod", "book", validator=book_validator)


print("----------------------------------------------------------------------")

create_book_collection()


def create_author_collection():
    author_validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["first_name", "last_name", "date_of_birth"],
            "properties": {
                "first_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "last_name": {
                    "bsonType": "string",
                    "description": "must be a string and is required"
                },
                "date_of_birth": {
                    "bsonType": "date",
                    "description": "must be a date is required"
                }
            }
        }
    }

    try:
        client.Library.create_collection("author")
    except Exception as e:
        print(f"[-] {e}")

    client.Library.command("collMod", "author", validator=author_validator)


create_author_collection()

print("----------------------------------------------------------------------")


def create_authors():
    authors = [
        {
            "first_name": "Priyanka",
            "last_name": "Bisht",
            "date_of_birth": datetime.datetime(1994, 11, 25)
        },
        {
            "first_name": "Abhijeet",
            "last_name": "Ranjan",
            "date_of_birth": datetime.datetime(1994, 1, 3)
        },
        {
            "first_name": "Abhishek",
            "last_name": "Ranjan",
            "date_of_birth": datetime.datetime(1991, 2, 3)
        },
        {
            "first_name": "Christie",
            "last_name": "Ranjan",
            "date_of_birth": datetime.datetime(2013, 2, 10)
        },
        {
            "first_name": "Yajur",
            "last_name": "Nagi",
            "date_of_birth": datetime.datetime(1994, 1, 3)
        },
    ]

    author_ids = client.Library.author.insert_many(authors).inserted_ids
    print("Collection 'author' created")
    return author_ids


author_ids = create_authors()

print("----------------------------------------------------------------------")

def create_books(author_ids):
    books = [
        {
            "title": "Life Of Bud",
            "authors": [author_ids[1], author_ids[0]],
            "publish_date": datetime.datetime(1994, 11, 25),
            "genre": "Non-Fiction",
            "copies": 20000
        },
        {
            "title": "Life Of Bada",
            "authors": [author_ids[0], author_ids[2]],
            "publish_date": datetime.datetime(1994, 1, 3),
            "genre": "Non-Fiction",
            "copies": 10
        },
        {
            "title": "Life Of Christie",
            "authors": [author_ids[1], author_ids[2]],
            "publish_date": datetime.datetime(2013, 2, 10),
            "genre": "Fiction",
            "copies": 100000
        },
        {
            "title": "Life Of BigB",
            "authors": [author_ids[3]],
            "publish_date": datetime.datetime(1991, 2, 3),
            "genre": "Non-Fiction",
            "copies": 232
        },
        {
            "title": "King Life",
            "authors": [author_ids[4], author_ids[1]],
            "publish_date": datetime.datetime(1995, 1, 3),
            "genre": "Fiction",
            "copies": 52532
        },
    ]

    book_ids = client.Library.book.insert_many(books).inserted_ids
    print("Collection 'book' created")
    return book_ids


book_ids = create_books(author_ids)

print("----------------------------------------------------------------------")

books_containing_b = client.Library.book.find({"title": {"$regex": "B{1}"}})
print("Books containing text 'B' printed")
pprint.pprint(list(books_containing_b))

print("----------------------------------------------------------------------")


# ------------------------------------------------------------------------------
# Aggregation and pipeline


authors_and_books = client.Library.author.aggregate([{
    "$lookup": {
        "from": "book",
        "localField": "_id",
        "foreignField": "authors",
        "as": "books"
    }
}])

print("Authors and books")
pprint.pprint(list(authors_and_books))

print("----------------------------------------------------------------------")


authors_book_count = client.Library.author.aggregate([{
    "$lookup": {
        "from": "book",
        "localField": "_id",
        "foreignField": "authors",
        "as": "books"
        }
    },
    {
    "$addFields": {
            "total_books": {"$size": "$books"}
        }
    },
    {
    "$project": {
        "first_name": 1, 
        "last_name": 1, 
        "total_books": 1, 
        "_id": 0}
    }
    ])

print("Authors book count")
pprint.pprint(list(authors_book_count))

print("----------------------------------------------------------------------")


authors_btw_28_29_and_books = client.Library.book.aggregate([{
        "$lookup": {
        "from": "author",
        "localField": "authors",
        "foreignField": "_id",
        "as": "authors"
        }
    },
    {
        "$set": {
            "authors": {
                "$map": {
                    "input": "$authors",
                    "in": {
                        "age": {
                            "$dateDiff": {
                                "startDate": "$$this.date_of_birth",
                                "endDate": "$$NOW",
                                "unit": "year"
                            }
                        },
                        "first_name": "$$this.first_name",
                        "last_name": "$$this.last_name"
                    }
                }
            }
        }
    },
    {
        "$match": {
            "$and" : [
                {"authors.age": {"$gte": 28}},
                {"authors.age": {"$lte": 29}}
                ]
        }
    },
    {
        "$sort": {
            "age": 1
        } 
    }
    
])

print("Authors between 28 and 29 years of age")
pprint.pprint(list(authors_btw_28_29_and_books))

print("----------------------------------------------------------------------")
