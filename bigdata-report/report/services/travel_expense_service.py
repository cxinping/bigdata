# -*- coding: utf-8 -*-
"""
差旅费异常检查

@author: WangShuo
"""

import csv
import json
import os
import time

#from report.commons.connect_kudu import prod_execute_sql, dis_connection
from report.commons.connect_kudu2 import prod_execute_sql,dis_connection

from report.commons.logging import get_logger
from report.commons.tools import match_address
from report.commons.settings import CONN_TYPE
from string import punctuation
from string import digits
import re
import jieba.analyse as analyse
import jieba

log = get_logger(__name__)


"""
差旅费
01_datamart_layer_007_h_cw_df.finance_travel_bill   

"""


def get_travel_keyword():
    """
    抽取差旅费的关键字
    :return:
    """

    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_travel_bill where commodityname is not null and commodityname !='' "
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    jiebaword = []
    words = []
    for record in records:
        record_str = str(record[0])

        words.append(record_str)

    words1 = ' '.join(words)
    words2 = re.sub(r'[{}]+'.format(punctuation + digits), '', words1)
    words3 = re.sub("[a-z]", "", words2)
    words4 = re.sub("[A-Z]", "", words3)

    # print(words4)

    jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")
    final_list = analyse.extract_tags(words4, topK=80, withWeight=False, allowPOS=())

    return final_list


if __name__ == "__main__":
    #main()
    keywords = get_travel_keyword()
    for keyword in keywords:
        print(keyword)
