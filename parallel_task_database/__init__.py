"""parallel_task_database. Provide
functions to manage tasks that are being run by jobs/workers running on a cluster.
"""
# Copyright 2018 Fred Hutchinson Cancer Research Center

# This file is to tell setuptools that this directory is a package

import os
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

def to_datetime(time_text):
    standard, field = time_text.split(".")
    fraction_seconds = field.split(" UTC")[0]
    if len(fraction_seconds) > 6:
       fraction_seconds = fraction_seconds[:6]
    return datetime.datetime.strptime(standard + "." + fraction_seconds + " UTC", "%Y-%m-%d %H:%M:%S.%f UTC")



def make_dependent_directory(filename):
    direc = os.path.dirname(filename)
    os.system("mkdir -p " + direc)


def stage_file(to_file=None, from_file=None):
    if None == to_file or None == from_file:
        raise Exception("ERROR either to_file or from_file not None")
    if "s3://" in to_file[:5] or "s3://" in from_file[:5]:
        params = [ "aws", "s3", "cp", from_file, to_file]
        invoke_system(params)
    else:
        params = [ "cp", from_file, to_file]
        invoke_system(params)

def to_staging_name(staging_dir, original_name):
    """Assuming that there are no naming collisions when populating the
    staging directory, append the basic filename to the staging directory
    name"""

    return os.path.join(staging_dir, os.path.basename(original_name))

def stage_file_to_dir(staging_dir, original_name):
    """Assuming that there are no naming collisions when populating the
    staging directory,
      1. derive staging name (this is returned)
      2. download/upload file to staging name (may assume directory exists)
    Return staging name for further use
    """

    staged_file = to_staging_name(staging_dir, original_name)
    stage_file(from_file=original_name, to_file=staged_file)
    return staged_file


def log_machine_name(log_file_name=None):
    if None == log_file_name:
        os.system("uname -a")
    else:
        os.system("uname -a >> " + log_file_name)
