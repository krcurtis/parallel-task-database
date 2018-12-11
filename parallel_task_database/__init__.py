"""parallel_task_database. Provide
functions to manage tasks that are being run by jobs/workers running on a cluster.
"""
# Copyright 2018 Fred Hutchinson Cancer Research Center

# This file is to tell setuptools that this directory is a package

from .mongo_uri import *
from .liteweight_worker import *


def populate_database(payloads, database, uri):
    client = pymongo.MongoClient(uri)
    db = client[database]
    for payload in payloads:
        print(payload)
        db.tasks.insert_one( {'payload':payload, 'processing':False})


def populate_database_cleanly(payloads, database, uri):
    client = pymongo.MongoClient(uri)
    db = client[database]
    db.tasks.delete_many({})
    for payload in payloads:
        db.tasks.insert_one( {'payload':payload, 'processing':False})


def get_task_collection_stats(client):
    results = []
    for name in client.database_names():
        if name != "admin" and name != "local":
            db = client[name]
            n = db.tasks.count({})
            results.append((name, n))
    return results
