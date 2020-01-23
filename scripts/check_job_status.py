#!/usr/bin/env python

# Copyright 2018 Fred Hutchinson Cancer Research Center
################################################################################
### check job status

import os
import re
import stat
import argparse
import datetime


import numpy as np
import pymongo
import tabulate

from parallel_task_database import mongo_uri
from parallel_task_database import get_task_collection_stats

################################################################################




def elapsed(workitem):
    td = workitem['stop'] - workitem['start']
    return td.total_seconds()

def is_processing(workitem):
    return workitem["processing"] and "stop" not in workitem


#def is_redo(workitem):
#    if "success" not in workitem:
#       return True
#    return workitem["processing"] and not workitem["success"]

def is_successful(workitem):
    return "success" in workitem and workitem["success"]

def is_known_failure(workitem):
    return "success" in workitem and not workitem["success"]


def trim_unsuccessful_job(workitem):
    revised = {}
    revised['processing'] = False
    keys_to_ignore = set(["stop", "success", "start", "jobid", "processing"])
    for k in workitem.keys():
        if k not in keys_to_ignore:
            revised[k] = workitem[k]
    return revised

def is_overdue(workitem, reference_time, mean_run_time, std_dev):
    if "stop" in workitem or "start" not in workitem:
        return False
    td = reference_time - workitem['start']
    elapsed = td.total_seconds()
    return elapsed > (mean_run_time + 2*std_dev)


def get_unprocessed_count(db):
    return db.tasks.count_documents({"processing":False})


def display_stats(records):
    print(tabulate.tabulate(records, headers=['Info', 'Status']))

def get_and_display_stats(db):
    now = datetime.datetime.utcnow()

    items = [i for i in db.tasks.find()]
    completed = [ i for i in items if "stop" in i]
    #unsuccesful_items = [ i for i in items if is_redo(i)]
    unfinished_items = [ i for i in items if not is_successful(i) and not is_processing(i) ]
    reset_items = [ i for i in items if  is_known_failure(i) or is_processing(i) ]
    elapsed_times = np.array([elapsed(i) for i in completed])

    mean_run_time = 0
    sigma = 0
    if len(elapsed_times) > 0:
        mean_run_time = elapsed_times.mean()
        sigma = elapsed_times.std(ddof=1)

    total_cpu_hours = 0
    if len(elapsed_times)> 0:
        total_cpu_hours = elapsed_times.sum() / 3600;


    overdue = [i for i in items if is_overdue(i, now, mean_run_time, sigma)]

    records = []
    records.append(('Total', len(items)))
    records.append(('Successful', len([i for i in items if is_successful(i)])))
    records.append(('Known failure', len([i for i in items if is_known_failure(i)])))
    records.append(('Overdue', len(overdue))) # (current elapsed run time greater than mean+2*sigma)

    records.append(('Completed', len(completed)))
    records.append(('Have not yet started', get_unprocessed_count(db)))
    records.append(('Running', len([i for i in items if is_processing(i)])))
    records.append(('Classified as un-completed or unsuccessful', len(unfinished_items)))


    records.append(('CPU-hours', total_cpu_hours))
    records.append(('Run times (mean)', '{mean} +/- {sigma}'.format(mean=mean_run_time, sigma=sigma)))
    display_stats(records)


def old_display_stats(records):
    print('Total:', len(items))
    print('Completed:', len(completed))
    print('CPU-hours:', total_cpu_hours)
    print('Run times (mean): {mean} +/- {sigma}'.format(mean=mean_run_time, sigma=sigma))
    print('Successful:', len([i for i in items if is_successful(i)]))
    print('Known failure:', len([i for i in items if is_known_failure(i)]))
    print('Have not yet started:', get_unprocessed_count(db))
    print('Running:', len([i for i in items if is_processing(i)]))
    print('Classified as un-completed or unsuccessful:', len(unfinished_items))
    print('Overdue (current elapsed run time greater than mean+2*sigma):', len(overdue))





parser = argparse.ArgumentParser(description='View summary stats tasks in database')
parser.add_argument('--list-queues', help='if specified, will only list possible task collections in the mongoDB database', default=False, action='store_true')
parser.add_argument('-d', '--database', help='mongoDB database', default="gecco_tasks")
args = parser.parse_args()

uri = mongo_uri()
client = pymongo.MongoClient(uri)
if args.list_queues:
    results = get_task_collection_stats(client)
    print(tabulate.tabulate(results, headers=['Task collection', 'n_tasks']))

else:
    db = client[args.database]
    get_and_display_stats(db)
