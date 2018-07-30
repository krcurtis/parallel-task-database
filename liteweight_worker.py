# Copyright 2018 Fred Hutchinson Cancer Research Center
################################################################################
### Framework for using MongoDB to get task parameters


import os
import stat
import time
import copy
import datetime
import sys
import traceback
import subprocess

from collections import OrderedDict

import pymongo
import numpy as np



################################################################################

DATABASE_ACCESS_FILE = os.path.join(os.environ['HOME'], '.mydb_gizmo_database')

def get_access(filename):
    access_mode = os.stat(DATABASE_ACCESS_FILE).st_mode
    if access_mode & (stat.S_IRWXG | stat.S_IRWXO) != 0:
        raise Exception("ERROR database access file has permissions for group or other")

    pw = open(DATABASE_ACCESS_FILE).readline().strip('\n')
    return pw



MONGO_URI = "mongodb://db_write:" + get_access(DATABASE_ACCESS_FILE) + "@mydb.fredhutch.org:32069"

################################################################################

def invoke_system(cmd_params, log_to_file=None):
    cmd = ' '.join(cmd_params)
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out_msg, err_msg = p.communicate()
    print("stdout:", out_msg)
    print("stderr:", err_msg)
    if None != log_to_file:
        with open(log_to_file, 'at') as f_out:
            f_out.write("cmd" + str( cmd_params) + "\n")
            f_out.write("stdout:" + str(out_msg) +  "\n")
            f_out.write("stderr:" + str(err_msg) + "\n")
    errcode  = p.returncode
    if 0 != errcode:
        raise Exception("ERROR: failed (returns {errcode}):".format(errcode=errcode) + cmd + '\n' + err_msg.decode(encoding='utf-8') + '\n' + out_msg.decode(encoding='utf-8'))
    return True

################################################################################



def get_job_params(database):
    found_task = False
    while not found_task:
        delay = np.random.randint(1, 10, dtype='int32')
        time.sleep(delay)

        print('Connecting...')
        client = pymongo.MongoClient(MONGO_URI)
        db = client[database]

        potential_task =  db.tasks.find_one({'processing':False})
        print(potential_task)
        if None == potential_task:
            print("Found no potentials tasks in database")
            return None
        update = copy.copy(potential_task)
        update['processing'] = True
        update['jobid'] = os.environ['SLURM_JOBID']
        update['start'] = datetime.datetime.utcnow()
        match_task = OrderedDict()
        match_task['_id'] = potential_task['_id']  # required for python3 why?
        match_task['processing'] = False
        result =  db.tasks.replace_one(match_task, update)
        if 1 == result.matched_count and 1 == result.modified_count:
            found_task = True
            print('Found task', update)
            return update
        else:
            print(potential_task)
            print('replace collision, matched={matched_count} modified={modified_count}'.format(matched_count=result.matched_count, modified_count=result.modified_count))
        client.close()
        print("Looping in check loop")
    return None


def close_job_params(database, job_params, success):
    client = pymongo.MongoClient(MONGO_URI)
    db = client[database]
    update = copy.copy(job_params)
    update['success'] = success
    update['stop'] = datetime.datetime.utcnow()
    result = db.tasks.replace_one(job_params, update)
    if 1 == result.matched_count and 1 == result.modified_count:
        print('Successfully updated database')
    else:
        print('Error in database update', update)




def main_loop(app_func, database):
    print('Starting main loop')
    job_params = get_job_params(database)
    if None == job_params:
        print("No job found")
        sys.exit(0)
    try:
        result = app_func(job_params)
    except Exception:
        # todo print stack trace ...
        result = False
        print(traceback.format_exc())

    close_job_params(database, job_params, result)
    if result:
        print("DONE[" + str(job_params) + "]")
