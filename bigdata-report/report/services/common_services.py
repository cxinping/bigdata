# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
from report.commons.connect_kudu import prod_execute_sql
from report.commons.logging import get_logger
from report.commons.mysql_pool import AsyncMysql, exec_insert
import asyncio, aiomysql

log = get_logger(__name__)

conn_type = 'test'  # prod


def insert_finance_shell_daily(daily_status, daily_start_date, daily_end_date, unusual_point, daily_source,
                               operate_desc, unusual_infor):
    """
    保存执行脚本或SQL的状态
    """
    daily_id = create_uuid()
    try:
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_shell_daily(daily_id, daily_status, daily_start_date, daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor) 
        values("{daily_id}", "{daily_status}", "{daily_start_date}", "{daily_end_date}" ,"{unusual_point}", "{daily_source}", "{operate_desc}", "{unusual_infor}" )
        """.replace('\n', '').replace('\r', '').strip()
        # log.info(sql)
        prod_execute_sql(conn_type=conn_type, sqltype='insert', sql=sql)
    except Exception as e:
        print(e)


def clean_finance_category_sign(unusual_id):
    del_sql = f'DELETE FROM 01_datamart_layer_007_h_cw_df.finance_category_sign WHERE unusual_id="{unusual_id}"  '
    prod_execute_sql(conn_type='test', sqltype='insert', sql=del_sql)


def operate_finance_category_sign(unusual_id, category_names, category_classify, sign_status='1'):
    """
    改变商品的选中状态
    :param unusual_id:
    :param category_names:  商品大类或商品关键字
    :param category_classify: 类别, 1 代表商品大类，2 代表商品关键字
    :return:
    """

    insert_sql = """INSERT INTO 01_datamart_layer_007_h_cw_df.finance_category_sign(id, category_name, category_classify , sign_status, unusual_id) VALUES"""
    values_sql = ''

    if len(category_names) == 1:
        id = create_uuid()
        values_sql = f'("{id}", "{category_names[0]}", "{category_classify}","{sign_status}" , "{unusual_id}" )'
    elif len(category_names) > 1:
        for idx, category in enumerate(category_names):
            id = create_uuid()
            values_sql = values_sql + f'("{id}", "{category}", "{category_classify}","{sign_status}" , "{unusual_id}" )'
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
            # if query_area_name == area_name:
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

    def query_receipt_city(self, area_name):
        """
        查询发票开票所在市
        :param area_name:
        :return:
        """

        if area_name is None:
            return None

        area_id, area_name, parent_id, grade = self.query_province(query_area_name=area_name)
        # print(area_id, area_name, parent_id, grade )

        if area_name is None or area_name == 'None':
            return None

        if grade and grade == '1':
            return area_name

        idx = 0
        if grade and grade != '2':

            while grade and grade != '2':
                idx = idx + 1

                if idx > 3:
                    return None

                area_id, area_name, parent_id, grade = self.query_previous_province(query_area_id=parent_id)
                # print('222 ', area_id, area_name, parent_id, grade)

                if grade and (grade == '2' or grade == '1'):
                    return area_name

        elif grade and grade == '2':
            return area_name

        return None


def pagination_finance_shell_daily_records(unusual_point=None):
    """
       分页查询shell脚本日志表的记录
       :param unusual_point: 检查点
       :return:
       """

    columns_ls = ['daily_id', 'daily_status', 'daily_start_date', 'daily_end_date', 'unusual_point', 'daily_source',
                  'operate_desc', 'unusual_infor']
    columns_str = ",".join(columns_ls)

    ###### 拼装查询SQL
    where_sql = 'WHERE '

    if unusual_point is None:
        where_sql = where_sql + ' 1=1'
    elif unusual_point:
        where_sql = where_sql + f' unusual_point = "{unusual_point}" '

    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_shell_daily "
    order_sql = ' ORDER BY daily_start_date DESC '
    sql = sql + where_sql + order_sql

    count_sql = 'SELECT count(a.daily_status) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type='test', sqltype='select', sql=count_sql)
    count_records = records[0][0]
    print('* count_records => ', count_records)

    # print(sql)

    return count_records, sql, columns_ls


class MySQLService:

    def __init__(self):
        pass

    def check_area(self, area_name_val, city_val, province_val):
        id, area_name, city, province = self.query_area_record(area_name_val)
        print(id, area_name, city, province)

        if id:
            self.insert_update_area(id=id, area_name=area_name_val, city=city_val, province=province_val)
        else:
            id = create_uuid()
            print(area_name_val, city_val, province_val)
            self.insert_update_area(id=id, area_name=area_name_val, city=city_val, province=province_val)

    def insert_update_area(self, id, area_name, city, province=None):
        sqllist = []

        if city and province:
            sql = f"""
            INSERT INTO areas(id, area_name, city, province) VALUES('{id}' ,'{area_name}', '{city}' , '{province}' ) 
            ON DUPLICATE KEY UPDATE area_name = '{area_name}', city= '{city}' , province = '{province}'        
            """
            print(sql)
            sqllist.append(sql)
        elif city and province is None:
            sql = f"""
            INSERT INTO areas(id, area_name, city) VALUES('{id}' ,'{area_name}', '{city}') 
            ON DUPLICATE KEY UPDATE area_name = '{area_name}', city= '{city}'    
            """
            print(sql)
            sqllist.append(sql)

        if len(sqllist) > 0:
            event_loop = asyncio.get_event_loop()
            event_loop.run_until_complete(exec_insert(event_loop, sqltype='insert', sqllist=sqllist))
            event_loop.close()

    def query_area_record(self, area_name):
        sql = f'SELECT id, area_name, city, province FROM areas WHERE area_name = "{area_name}" '
        sqllist = []
        sqllist.append(sql)

        event_loop = asyncio.get_event_loop()
        task = event_loop.create_task(exec_insert(event_loop, sqltype='select', sqllist=sqllist))
        event_loop.run_until_complete(task)
        event_loop.close()

        results = task.result()
        if results:
            for rs in results:
                if rs.result():
                    for item in rs.result():
                        id = item['id']
                        area_name = item['area_name']
                        city = item['city']
                        province = item['area_name']
                        return id, area_name, city, province

        return None, None, None, None
