"""parallel_task_database. Provide
functions to manage tasks that are being run by jobs/workers running on a cluster.
"""
# Copyright 2018 Fred Hutchinson Cancer Research Center

# This file is to tell setuptools that this directory is a package

from .mongo_uri import *
from .liteweight_worker import *

