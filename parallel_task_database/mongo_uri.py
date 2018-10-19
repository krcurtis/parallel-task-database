# Copyright 2018 Fred Hutchinson Cancer Research Center
################################################################################
### Framework for using MongoDB to get task parameters


import os
import stat
import json



################################################################################



def mongo_uri(database_access_file=os.path.join(os.environ['HOME'], '.mongo_database')):
    access_mode = os.stat(database_access_file).st_mode
    if access_mode & (stat.S_IRWXG | stat.S_IRWXO) != 0:
        raise Exception("ERROR database access file has permissions for group or other")
    
    in_file = open(database_access_file)
    d = json.load(in_file)

    uri = "mongodb://{user}:{password}@{host}:{port}".format(**d)
    return uri
