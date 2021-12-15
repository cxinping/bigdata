# -*- coding: utf-8 -*-
"""
会议费异常检查
@author: WangShuo

01_datamart_layer_007_h_cw_df.finance_meeting_bill
"""

import time

# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql

from report.commons.logging import get_logger
from report.commons.tools import match_address
from report.commons.db_helper import query_kudu_data
from report.services.vehicle_expense_service import cal_commodityname_function
from report.commons.tools import not_empty
from report.commons.db_helper import Pagination, db_fetch_to_dict
import jieba.analyse as analyse
import jieba
from string import punctuation
from string import digits
import re
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

"""
会议费
01_datamart_layer_007_h_cw_df.finance_meeting_bill  

"""


def query_checkpoint_26_commoditynames():
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where commodityname is not null and commodityname != "" '
    rd_df = query_kudu_data(sql=sql, columns=columns_ls, conn_type=CONN_TYPE)
    # print(len(rd_df))

    rd_df['category_class'] = rd_df.apply(lambda rd_df: cal_commodityname_function(rd_df['commodityname']), axis=1)
    # print(rd_df)

    category_class_ls = rd_df['category_class'].tolist()
    category_class_ls = list(filter(not_empty, category_class_ls))

    # 去重
    category_class_ls = list(set(category_class_ls))

    # print(len(category_class_ls))
    # print(category_class_ls)

    return category_class_ls


def get_conference_bill_jiebaword():
    """
    抽取 会议费的关键字
    :return:
    """

    commodityname_ls = query_checkpoint_26_commoditynames()
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where commodityname is not null and commodityname !=''"
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    jiebaword = []
    words = []
    for record in records:
        record_str = str(record[0])

        for commodityname in commodityname_ls:
            if record_str.find(commodityname) > -1:
                record_str = record_str.replace(commodityname, '')

        words.append(record_str)

    words1 = ' '.join(words)
    words2 = re.sub(r'[{}]+'.format(punctuation + digits), '', words1)
    words3 = re.sub("[a-z]", "", words2)
    words4 = re.sub("[A-Z]", "", words3)

    # print(words4)

    jieba.analyse.set_stop_words("/you_filed_algos/app/report/algorithm/stop_words.txt")
    jieba.analyse.set_idf_path("/you_filed_algos/app/report/algorithm/userdict.txt")
    final_list = analyse.extract_tags(words4, topK=50, withWeight=False, allowPOS=())

    return final_list


def pagination_conference_records(categorys, good_keywords):
    """
    分页查询会议费的记录
    :param categorys: 商品分类列表
    :param good_keywords: 商品关键字列表
    :return:
    """
    columns_ls = ['bill_id', 'commodityname', 'bill_type_name']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_meeting_bill "

    count_sql = 'SELECT count(a.bill_id) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    if categorys:
        if good_keywords:
            categorys.extend(good_keywords)
    else:
        categorys = []
        if good_keywords:
            categorys.extend(good_keywords)

    if len(categorys) == 1:
        where_sql = where_sql + f'commodityname LIKE "%{categorys[0]}%"'
    elif len(categorys) > 1:
        for idx, category in enumerate(categorys):
            tmp_sql = f'commodityname LIKE "%{category}%"'

            if idx != len(categorys) - 1:
                condition_sql = condition_sql + tmp_sql + ' OR '
            else:
                condition_sql = condition_sql + tmp_sql

        where_sql = where_sql + condition_sql
    elif len(categorys) == 0:
        where_sql = where_sql + ' 1=1'

    order_sql = ' ORDER BY bill_id ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls


if __name__ == "__main__":
    # records = query_checkpoint_26_commoditynames()
    # print(records)
    #
    # final_list = get_conference_bill_jiebaword()
    # for word in final_list:
    #     print(word)

    categorys = ['运输服务', '包装饮用水', ]  # ['运输服务', '包装饮用水', '住宿服务', '计算机配套产品' ]
    good_keywords = ['手册']  # ['手册', '会议费', '荣誉证书']
    count_records, sql, columns_ls = pagination_conference_records(categorys, good_keywords)
    print(count_records, sql, columns_ls)

    current_page = 1
    page_obj = Pagination(current_page=current_page, all_count=count_records, per_page_num=10)
    records = page_obj.exec_sql(sql, columns_ls)
    print(records)
