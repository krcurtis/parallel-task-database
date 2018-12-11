#!/usr/bin/env python

# Copyright 2018 Fred Hutchinson Cancer Research Center
################################################################################
### Reset uncompleted jobs, so that workers can 'see' them, and process

import os
import re
import stat
import argparse
import datetime
import sys

import numpy as np
import pymongo

from parallel_task_database import mongo_uri
from parallel_task_database import get_task_collection_stats

################################################################################

def check_if_sure(response):
    if "yes" != response:
        if "no" == response:
            print("Terminating")
            sys.exit(0)
        else:
            print("Response not understood\nTerminating")
            sys.exit(0)


parser = argparse.ArgumentParser(description='Remove (or clear out) tasks in database')
parser.add_argument('-d', '--database', help='MongoDB collection holding the tasks to be removed', default="gecco_tasks")
parser.add_argument('--all-collections', help='remove all task collection', default=False, action='store_true')
args = parser.parse_args()


if not args.all_collections:
    response = input("This will clear the jobs from the database {}\nAre you sure (type yes or no)? ".format(args.database))
    check_if_sure(response)

    uri = mongo_uri()
    client = pymongo.MongoClient(uri)
    db = client[args.database]

    print("Database delete", db.tasks.delete_many({}))

else:
    response = input("This will clear all task collections from the database\nAre you sure (type yes or no)? ")
    check_if_sure(response)

    uri = mongo_uri()
    client = pymongo.MongoClient(uri)
    dbs = get_task_collection_stats(client)

    for name,n in dbs:
        client.drop_database(name)


    print("DONE")





