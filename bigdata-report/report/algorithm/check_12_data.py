# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
import os, time
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
import pandas as pd
from report.commons.connect_kudu import prod_execute_sql

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

dest_dir = '/you_filed_algos/prod_kudu_data/checkpoint12'
dest_file = dest_dir + '/check_12_data.txt'


class Check12Service():

    def __init__(self):
        pass

    def init_file(self):
        if not os.path.exists(dest_dir):
            os.makedirs(dest_dir)

        if os.path.exists(dest_file):
            os.remove(dest_file)

    def save_data(self):
        self.init_file()

    def analyze_data_data(self):
        pass


if __name__ == "__main__":
    check12_service = Check12Service()
