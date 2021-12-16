# -*- coding: utf-8 -*-
"""
车辆使用费异常检查

@author: WangShuo
"""


from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.db_helper import query_kudu_data
from report.commons.logging import get_logger
from report.commons.tools import not_empty

import jieba.analyse as analyse
import jieba
from string import punctuation
from string import digits
import re
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)

"""
车辆使用费

01_datamart_layer_007_h_cw_df.finance_car_bill 

"""


def query_checkpoint_55_commoditynames():
    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !="" '
    rd_df = query_kudu_data(sql=sql, columns=columns_ls, conn_type=CONN_TYPE)

    rd_df['category_class'] = rd_df.apply(lambda rd_df: cal_commodityname_function(rd_df['commodityname']), axis=1)
    # print(rd_df)

    category_class_ls = rd_df['category_class'].tolist()
    category_class_ls = list(filter(not_empty, category_class_ls))
    # 去重
    category_class_ls = list(set(category_class_ls))
    # print(len(category_class_ls), category_class_ls)
    return category_class_ls


def cal_commodityname_function(commodityname):
    """
    计算大类
    :param commodityname:
    :return:
    """
    category_class = None
    if commodityname:
        # print('**** 111 commodityname ==> ', commodityname, type(commodityname))

        if commodityname.find('*') > -1 and commodityname.find(',') > -1:
            commodityname_ls = commodityname.split(',')
            commodityname_ls = list(filter(not_empty, commodityname_ls))
            first_category = str(commodityname_ls[0]).strip()

            category_ls = first_category.split('*')
            category_ls = list(filter(not_empty, category_ls))
            category_class = str(category_ls[0]).strip()

        elif commodityname.find('*') > -1:
            category_ls = commodityname.split('*')
            category_ls = list(filter(not_empty, category_ls))
            category_class = str(category_ls[0]).strip()
        # else:
        #     category_class = commodityname
        #     print('**** 222 category_class ==> ' , category_class)

    return category_class


def get_car_bill_jiebaword():
    """
    抽取车辆使用费的关键字
    :return:
    """

    commodityname_ls = query_checkpoint_55_commoditynames()
    sql = "select distinct commodityname from 01_datamart_layer_007_h_cw_df.finance_car_bill where commodityname is not null and commodityname !=''"
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
    final_list = analyse.extract_tags(words4, topK=80, withWeight=False, allowPOS=())

    return final_list


def pagination_car_records(categorys, good_keywords):
    """
    分页查询车辆使用费的记录
    :param categorys: 商品分类列表
    :param good_keywords: 商品关键字列表
    :return:
    """
    columns_ls = ['bill_id', 'commodityname', 'bill_type_name']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_official_bill "

    count_sql = 'SELECT count(a.bill_id) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    print('* count_records => ', count_records)

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
    # main()

    # records = query_checkpoint_55_commoditynames()
    # print(records)

    final_list = get_car_bill_jiebaword()

    for word in final_list:
        print(word)
