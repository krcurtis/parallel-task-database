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

################################################################################

DATABASE_ACCESS_FILE = os.path.join(os.environ['HOME'], '.mydb_gizmo_database')

def get_access(filename):
    access_mode = os.stat(DATABASE_ACCESS_FILE).st_mode
    if access_mode & (stat.S_IRWXG | stat.S_IRWXO) != 0:
        raise Exception("ERROR database access file has permissions for group or other")

    pw = open(DATABASE_ACCESS_FILE).readline().strip('\n')
    return pw



MONGO_URI = "mongodb://db_write:" + get_access(DATABASE_ACCESS_FILE) + "@mydb:32069"

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



client = pymongo.MongoClient(MONGO_URI)
db = client[args.database]


print("Database delete", db.tasks.delete_many({}))


# items = [i for i in db.tasks.find()]
#        result = db.tasks.replace_one(i, revised)





