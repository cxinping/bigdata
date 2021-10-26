# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger

log = get_logger(__name__)


def insert_finance_shell_daily(daily_status, daily_start_date, daily_end_date, unusual_point, daily_source,
                               operate_desc, unusual_infor):
    daily_id = create_uuid()
    sql = f"""
    insert into 01_datamart_layer_007_h_cw_df.finance_shell_daily(daily_id, daily_status, daily_start_date, daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor) 
    values('{daily_id}', '{daily_status}', '{daily_start_date}', '{daily_end_date}' ,'{unusual_point}', '{daily_source}', '{operate_desc}', '{unusual_infor}' )
    """.replace('\n', '').replace('\r', '').strip()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)


def update_finance_category_sign(unusual_id, category_names):
    """
    改变商品的选中状态
    :param unusual_id:
    :param category_names:
    :return:
    """
    del_sql = f'delete from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}"'
    prod_execute_sql(conn_type='test', sqltype='insert', sql=del_sql)
    insert_sql = """INSERT INTO 01_datamart_layer_007_h_cw_df.finance_category_sign(id, category_name, sign_status, unusual_id) VALUES"""
    values_sql = ''

    if len(category_names) == 1:
        id = create_uuid()
        values_sql = f'("{id}", "{category_names[0]}", "1" , "{unusual_id}" )'
    elif len(category_names) > 1:
        for idx, category in enumerate(category_names):
            id = create_uuid()
            values_sql = values_sql + f'("{id}", "{category}", "1" , "{unusual_id}" )'
            if idx != len(category_names) - 1:
                values_sql = values_sql + ','

    insert_sql = insert_sql + values_sql
    #print(insert_sql)
    prod_execute_sql(conn_type='test', sqltype='insert', sql=insert_sql)


def query_finance_category_sign(unusual_id):
    """
    查询选中状态的商品
    :param unusual_id:
    :return:
    """
    sel_sql = f'select category_name from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}"'
    category_names = prod_execute_sql(conn_type='test', sqltype='select', sql=sel_sql)

    category_name_ls = []
    for category in category_names:
        category_name = category[0]
        category_name_ls.append(category_name)

    return category_name_ls


if __name__ == "__main__":
    daily_status = 'ok'
    daily_start_date = '2021-10-25 17:05'
    daily_end_date = '2021-10-25 20:05'
    unusual_point = '42'
    daily_source = 'sql'
    operate_desc = 'aaabbbccc'
    unusual_infor = 'ggggggggggggg'

    # insert_finance_shell_daily(daily_status, daily_start_date,daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor)

    unusual_id = '2'
    category_names = ['f', 'd', 'e']  # ['a' , 'b']
    #update_finance_category_sign(unusual_id, category_names)

    records = query_finance_category_sign(unusual_id=unusual_id)
    print(records)

    print('--- ok ---')




