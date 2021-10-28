# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger

log = get_logger(__name__)


def insert_finance_shell_daily(daily_status, daily_start_date, daily_end_date, unusual_point, daily_source,
                               operate_desc, unusual_infor):
    """
    保存执行脚本或SQL的状态
    """
    daily_id = create_uuid()
    sql = f"""
    insert into 01_datamart_layer_007_h_cw_df.finance_shell_daily(daily_id, daily_status, daily_start_date, daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor) 
    values('{daily_id}', '{daily_status}', '{daily_start_date}', '{daily_end_date}' ,'{unusual_point}', '{daily_source}', '{operate_desc}', '{unusual_infor}' )
    """.replace('\n', '').replace('\r', '').strip()
    prod_execute_sql(conn_type='test', sqltype='insert', sql=sql)


def update_finance_category_sign(unusual_id, category_names, category_classify):
    """
    改变商品的选中状态
    :param unusual_id:
    :param category_names:  商品大类或商品关键字
    :param category_classify: 类别, 1 代表商品大类，2 代表商品关键字
    :return:
    """
    del_sql = f'delete from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}" and category_classify="{category_classify}" '
    prod_execute_sql(conn_type='test', sqltype='insert', sql=del_sql)
    insert_sql = """INSERT INTO 01_datamart_layer_007_h_cw_df.finance_category_sign(id, category_name, category_classify , sign_status, unusual_id) VALUES"""
    values_sql = ''

    if len(category_names) == 1:
        id = create_uuid()
        values_sql = f'("{id}", "{category_names[0]}", "{category_classify}","1" , "{unusual_id}" )'
    elif len(category_names) > 1:
        for idx, category in enumerate(category_names):
            id = create_uuid()
            values_sql = values_sql + f'("{id}", "{category}", "{category_classify}","1" , "{unusual_id}" )'
            if idx != len(category_names) - 1:
                values_sql = values_sql + ','

    insert_sql = insert_sql + values_sql
    #print(insert_sql)
    prod_execute_sql(conn_type='test', sqltype='insert', sql=insert_sql)


def query_finance_category_sign(unusual_id, category_classify):
    """
    查询选中状态的商品
    :param unusual_id:
    :param category_classify: 类别, 1 代表商品大类，2 代表商品关键字
    :return:
    """
    sel_sql = f'select category_name from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}" and category_classify="{category_classify}" '
    category_names = prod_execute_sql(conn_type='test', sqltype='select', sql=sel_sql)

    category_name_ls = []
    for category in category_names:
        category_name = category[0]
        category_name_ls.append(str(category_name))

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

    unusual_id = '42'
    category_names = ['d']  # ['a' , 'b']
    category_classify = '001'
    #update_finance_category_sign(unusual_id, category_names, category_classify)

    category_classify = '2'
    records = query_finance_category_sign(unusual_id=unusual_id, category_classify=category_classify)
    print(records)

    print('--- ok ---')




