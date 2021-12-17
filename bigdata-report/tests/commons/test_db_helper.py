# -*- coding: utf-8 -*-
from report.commons.db_helper import *


def demo1():
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


if __name__ == '__main__':
    demo1()
