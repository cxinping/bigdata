# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.db_helper import db_fetch_to_dict
import os

log = get_logger(__name__)

"""
流程表业务处理



"""


def step_08():
    remote_path = '/you_filed_algos/app/shell/'
    jixiao_bangong_file = remote_path + 'jixiao_bangong.sh'
    jixiao_chailv_file = remote_path + 'jixiao_chailv.sh'
    jixiao_cheliang_file = remote_path + 'jixiao_cheliang.sh'
    jixiao_huiyi_file = remote_path + 'jixiao_huiyi.sh'

    os.system(f'sh {jixiao_bangong_file}')


step_08()
