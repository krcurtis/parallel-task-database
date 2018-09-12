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

################################################################################


parser = argparse.ArgumentParser(description='Remove (or clear out) tasks in database')
parser.add_argument('-d', '--database', help='mongoDB database or collection', default="gecco_tasks")
args = parser.parse_args()




response = input("This will clear the jobs from the database {}\nAre you sure (type yes or no)? ".format(args.database))
if "yes" != response:
    if "no" == response:
        print("Terminating")
        sys.exit(0)
    else:
        print("Response not understood\nTerminating")
        sys.exit(0)


uri = mongo_uri()
client = pymongo.MongoClient(uri)
db = client[args.database]


print("Database delete", db.tasks.delete_many({}))


# items = [i for i in db.tasks.find()]
#        result = db.tasks.replace_one(i, revised)





