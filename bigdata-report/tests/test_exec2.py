# -*- coding: utf-8 -*-

from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.db_helper import query_kudu_data
import time
import pandas as pd
import os
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import threading
from report.services.common_services import ProvinceService
from report.commons.test_hdfs_tools import HDFSTools as Test_HDFSTools
from report.commons.tools import list_of_groups

log = get_logger(__name__)

import sys

sys.path.append('/you_filed_algos/app')

def test(x, y):
    result = x + y
    print(f'inner result13={result}')
    return result


result13 = test(x,y)
#result13 = test(x=1,y=2)
print(f'outter result13={result13}')

