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
    # print(insert_sql)
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


class ProvinceService:

    def __init__(self):
        sel_all_sql = f"select area_id, area_name, parent_id, grade from 01_datamart_layer_007_h_cw_df.finance_province_city "
        self.province_records = prod_execute_sql(conn_type='test', sqltype='select', sql=sel_all_sql)

    def query_previous_province(self, query_area_id):
        if query_area_id is None:
            return None, None, None, None

        for record in self.province_records:
            area_id = str(record[0]) if record[0] else None

            # print(area_id, area_name, parent_id, grade)
            if area_id and query_area_id == area_id:
                area_name = str(record[1]) if record[1] else None
                parent_id = str(record[2]) if record[2] else None
                grade = str(record[3]) if record[3] else None

                return area_id, area_name, parent_id, grade

        return None, None, None, None

    def query_province(self, query_area_name):
        if query_area_name is None:
            return None, None, None, None

        for record in self.province_records:
            area_name = str(record[1]) if record[1] else None

            # print(area_id, area_name, parent_id, grade)
            #if query_area_name == area_name:
            if area_name.find(query_area_name) > -1:
                area_id = str(record[0]) if record[0] else None
                parent_id = str(record[2]) if record[2] else None
                grade = str(record[3]) if record[3] else None

                return area_id, area_name, parent_id, grade

        return None, None, None, None

    def query_belong_province(self, area_name):
        if area_name is None:
            return None

        area_id, area_name, parent_id, grade = self.query_province(query_area_name=area_name)
        if area_name is None or area_name == 'None':
            return None

        idx = 0
        if grade and grade != '1':

            while grade and grade != '1':
                idx = idx + 1

                if idx > 3:
                    return None

                area_id, area_name, parent_id, grade = self.query_previous_province(query_area_id=parent_id)
                if grade and grade == '1':
                    return area_name

        elif grade and grade == '1':
            return area_name

        return None


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
    # update_finance_category_sign(unusual_id, category_names, category_classify)

    # category_classify = '2'
    # records = query_finance_category_sign(unusual_id=unusual_id, category_classify=category_classify)
    # print(records)

    province_service = ProvinceService()
    #area_name = '金湖县'
    # province_service.query_province(area_name)
    # area_id = '510000'
    # area_id, area_name, parent_id, grade = province_service.query_previous_province(query_area_id=area_id)
    # print(area_id, area_name, parent_id, grade)
    area_name = '南川区'
    province_name = province_service.query_belong_province(area_name)
    print(province_name)

    # print('--- ok ---')
    # print('中原区'.find('中'))












