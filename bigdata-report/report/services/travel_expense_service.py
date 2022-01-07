# -*- coding: utf-8 -*-
"""
差旅费异常检查

@author: WangShuo
"""

from report.services.vehicle_expense_service import cal_commodityname_function
from report.commons.db_helper import query_kudu_data
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.tools import not_empty
from report.commons.logging import get_logger
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


def query_travel_commoditynames():
    """
    查询差旅费的大类
    :return:
    """
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where commodityname is not null and commodityname != "" '
    rd_df = query_kudu_data(sql=sql, columns=columns_ls, conn_type=CONN_TYPE)
    rd_df['category_class'] = rd_df.apply(lambda rd_df: cal_commodityname_function(rd_df['commodityname']), axis=1)

    category_class_ls = rd_df['category_class'].tolist()
    category_class_ls = list(filter(not_empty, category_class_ls))

    # 去重
    category_class_ls = list(set(category_class_ls))

    return category_class_ls


if __name__ == "__main__":
    #print('--- 差旅费发票的关键字 ---')
    # keywords = get_travel_keyword()
    # for keyword in keywords:
    #     print(keyword)

    print('--- 差旅费发票的大类 ---')
    names = query_travel_commoditynames()
    for name in names:
        print(name)



