# -*- coding: utf-8 -*-

import pandas as pd
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE

from report.commons.logging import get_logger

log = get_logger(__name__)


def query_kudu_data(sql=None, columns=[], conn_type=CONN_TYPE):
    """
    发票日期异常检查
    :return:
    """

    #print(sql, columns, conn_type)

    records = prod_execute_sql(conn_type=conn_type, sqltype='select', sql=sql)
    # log.info('***' * 20)
    log.info('*** query_kudu_data => ' + str(len(records)))
    # log.info('***' * 20)
    #print('')

    dataFromKUDU = []
    for item in records:
        record = []
        if columns:
            # for idx in range(len(columns)):
            for idx, _ in enumerate(columns):
                # print(item[idx], type(item[idx]))

                if str(item[idx]) == "None":
                    record.append(None)
                elif str(type(item[idx])) == "<java class 'JDouble'>":
                    record.append(float(item[idx]))
                elif str(type(item[idx])) == "<java class 'java.lang.Long'>":
                    record.append(float(item[idx]))
                else:
                    record.append(str(item[idx]))

        dataFromKUDU.append(record)

    del records

    df = pd.DataFrame(data=dataFromKUDU, columns=columns)
    return df


def db_fetch_to_dict(sql, columns=[]):
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
    result = []

    if len(records) == 1:
        result_row = dict(zip(columns, records[0]))
        for key in result_row.keys():
            if str(result_row[key]) == "None":
                result_row[key] = None
            elif str(type(result_row[key])) == "<java class 'JDouble'>":
                result_row[key] = float(result_row[key])
            elif str(type(result_row[key])) == "<java class 'java.lang.Long'>":
                result_row[key] = str(result_row[key])
            else:
                result_row[key] = str(result_row[key])

        result.append(result_row)
    else:
        for record in records:
            result_row = dict(zip(columns, record))
            for key in result_row.keys():
                if str(result_row[key]) == "None":
                    result_row[key] = None
                elif str(type(result_row[key])) == "<java class 'JDouble'>":
                    result_row[key] = float(result_row[key])
                elif str(type(result_row[key])) == "<java class 'java.lang.Long'>":
                    result_row[key] = str(result_row[key])
                else:
                    result_row[key] = str(result_row[key])

            result.append(result_row)

    return result


class Pagination(object):
    def __init__(self, current_page=1, all_count=100, per_page_num=10, pager_count=11):
        """
        封装分页相关数据
        :param current_page: 当前页
        :param all_count:    数据库中的数据总条数
        :param per_page_num: 每页显示的数据条数
        :param pager_count:  最多显示的页码个数
        """
        try:
            current_page = int(current_page)
        except Exception as e:
            current_page = 1

        if current_page < 1:
            current_page = 1

        self.current_page = current_page

        self.all_count = all_count
        self.per_page_num = per_page_num

        # 总页码
        all_pager, tmp = divmod(all_count, per_page_num)
        if tmp:
            all_pager += 1
        self.all_pager = all_pager

        self.pager_count = pager_count
        self.pager_count_half = int((pager_count - 1) / 2)

    def exec_sql(self, sql, columns=[]):
        page_sql = sql + f' limit {self.per_page_num} offset {self.start}'
        log.info(page_sql)
        records = db_fetch_to_dict(page_sql, columns)
        return records

    @property
    def get_all_pager(self):
        """
        总页码
        :return:
        """
        return self.all_pager

    @property
    def start(self):
        return (self.current_page - 1) * self.per_page_num

    @property
    def end(self):
        return self.current_page * self.per_page_num


if __name__ == '__main__':
    pass

    # book_list = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26]
    #
    # current_page = 2
    # all_count = len(book_list)
    # page_obj = Pagination(current_page=current_page, all_count=all_count, per_page_num=10)
    # page_queryset = book_list[page_obj.start:page_obj.end]
    # print(page_queryset)
    # print(page_obj.start, page_obj.end)
    # print(page_obj.get_all_pager)
    #
    # sql = 'select unusual_id,cost_project from 01_datamart_layer_007_h_cw_df.finance_unusual order by unusual_id asc'
    # columns = ['unusual_id', 'cost_project']
    # page_obj.exec_sql(sql, columns)
    #
    # print('*' * 50)
    # columns_ls = ['bill_id', 'commodityname', 'bill_type_name']
    # columns_str = ",".join(columns_ls)
    # sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_meeting_bill limit 1"
    # db_fetch_to_dict(sql, columns_ls)

    columns_ls = ['commodityname']
    columns_str = ",".join(columns_ls)

    sql = f'select distinct {columns_str} from 01_datamart_layer_007_h_cw_df.finance_meeting_bill where commodityname is not null and commodityname != "" '
    rd_df = query_kudu_data(sql=sql, columns=columns_ls, conn_type=CONN_TYPE)
