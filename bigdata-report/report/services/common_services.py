# -*- coding: utf-8 -*-
from report.commons.tools import create_uuid
# from report.commons.connect_kudu import prod_execute_sql
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE

from report.commons.logging import get_logger
from report.commons.mysql_pool import AsyncMysql, exec_insert
from report.commons.db_helper import db_fetch_to_dict
import asyncio, aiomysql

log = get_logger(__name__)


def query_billds_finance_all_targets(unusual_id):
    bill_ids = []
    try:
        sql = f'select distinct bill_id from analytic_layer_zbyy_sjbyy_003_cwzbbg.finance_all_targets where unusual_id="{unusual_id}" '
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        # print(len(records))

        for idx, record in enumerate(records):
            # print(record)
            bill_id = record[0]
            bill_ids.append(bill_id)

        return bill_ids
    except Exception as e:
        print(e)
        return []


def insert_finance_shell_daily(daily_status, daily_start_date, daily_end_date, unusual_point, daily_source,
                               operate_desc, unusual_infor, task_status='doing'):
    """
    保存执行脚本或SQL的状态
    """
    daily_id = create_uuid()
    try:
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_shell_daily(daily_id, daily_status, daily_start_date, daily_end_date, unusual_point, daily_source, operate_desc, unusual_infor,task_status) 
        values("{daily_id}", "{daily_status}", "{daily_start_date}", "{daily_end_date}" ,"{unusual_point}", "{daily_source}", "{operate_desc}", "{unusual_infor}", "{task_status}" )
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return daily_id
    except Exception as e:
        print(e)


def update_finance_shell_daily(daily_id, daily_end_date='', task_status='done', operate_desc=''):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.finance_shell_daily SET task_status="{task_status}", daily_end_date="{daily_end_date}",operate_desc="{operate_desc}" WHERE daily_id="{daily_id}"
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return daily_id
    except Exception as e:
        print(e)


def update_finance_shell_daily_doing_status():
    # print('--- update_finance_shell_daily_doing_status ----')
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.finance_shell_daily SET task_status="cancel", unusual_infor="系统重启，取消正在执行的执行检查点任务" WHERE task_status="doing" 
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
    except Exception as e:
        print(e)


def query_finance_shell_daily_status(unusual_point, task_status='doing'):
    try:
        sql = f"""
        SELECT unusual_point, task_status FROM 01_datamart_layer_007_h_cw_df.finance_shell_daily WHERE unusual_point="{unusual_point}" AND task_status="{task_status}" 
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        # print(records)

        if records and len(records) > 0:
            return records[0]
        return None

    except Exception as e:
        print(e)


def clean_finance_category_sign(unusual_id):
    del_sql = f'DELETE FROM 01_datamart_layer_007_h_cw_df.finance_category_sign WHERE unusual_id="{unusual_id}"  '
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=del_sql)


def operate_finance_category_sign(unusual_id, category_names, category_classify, sign_status='1'):
    """
    改变商品的选中状态
    :param unusual_id:
    :param category_names:  商品大类或商品关键字
    :param category_classify: 类别, 01 代表商品大类，02 代表商品关键字
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
    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=insert_sql)


def query_finance_category_sign(unusual_id, category_classify):
    """
    查询选中状态的商品
    :param unusual_id:
    :param category_classify: 类别, 01 代表商品大类，02 代表商品关键字
    :return:
    """
    sel_sql = f'select category_name from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}" and category_classify="{category_classify}" '
    category_names = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)

    category_name_ls = []
    for category in category_names:
        category_name = category[0]
        category_name_ls.append(str(category_name))

    return category_name_ls


def query_finance_category_signs(unusual_id, category_classify):
    """
    查询选中状态的商品
    :param unusual_id:
    :param category_classify: 类别, 01 代表商品大类，02 代表商品关键字
    :return:
    """

    columns_ls = ['category_name', 'sign_status']
    columns_str = ",".join(columns_ls)
    sel_sql = f'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_category_sign where unusual_id="{unusual_id}" and category_classify="{category_classify}" ORDER BY sign_status DESC ,category_name DESC'
    records = db_fetch_to_dict(sql=sel_sql, columns=columns_ls)

    for record in records:
        record['sign_status'] = int(record['sign_status'])

    return records


class FinanceAdministrationService:
    """
    行政区域表
    """

    def __init__(self):
        columns = ['area_division_code', 'province', 'city', 'county', 'province_code', 'city_code', 'county_code']
        columns_str = ",".join(columns)
        sel_sql = f"select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_administration "
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)
        # print(len(records))

        self.finance_records = []

        for record in records:
            result_row = dict(zip(columns, record))
            #print(result_row)

            for key, value in result_row.items():
                result_row[key] = str(value)

            self.finance_records.append(result_row)


    def query_areas(self, sales_taxno):
        """
       根据纳锐人识别号查找 开票所在的 省，市，区/县
       :param sales_taxno: 纳税人识别号
       :return: 
       """

        if sales_taxno is None or sales_taxno == 'None':
            return None, None, None

        if sales_taxno and len(sales_taxno) not in [15, 20, 18]:
            return None, None, None

        sales_taxno_str = None
        if len(sales_taxno) == 15 or len(sales_taxno) == 20:
            sales_taxno_str = sales_taxno[:6]
        elif len(sales_taxno) == 18:
            sales_taxno_str = sales_taxno[2:8]

        #print('sales_taxno_str=',sales_taxno_str)

        if sales_taxno_str:
            rst = self.query_accurate_areas(sales_taxno_str)

            #log.info(rst)

            # 如果精确查找省，市，县 这一级的行政单位没有找到，就模糊查找省或市这级的行政单位
            if rst[0] is not None:
                return rst
            else:
                rst = self.query_blur_areas(sales_taxno_str)
                return rst

        return None, None, None

    def query_blur_areas(self, area_division_code):
        """
        根据纳锐人识别号, 模糊 查找 开票所在的 省，市，区/县
        :param area_division_code: 行政区划代码
        :return:
        """
        code_part1, code_part2, code_part3 = None, None, None
        code_part1 = area_division_code[0:2]
        code_part2 = area_division_code[2:4]
        code_part3 = area_division_code[4:6]

        # print(code_part1, code_part2, code_part3)

        for idx, item in enumerate(self.finance_records):
            # 只匹配 '省'和'市' 这级别的行政单位
            if code_part1 == item['province_code'] and code_part2 == item['city_code']:
                return str(item['province']), str(item['city']), None

        for idx, item in enumerate(self.finance_records):
            # 只匹配 '省' 这级别的行政单位
            if code_part1 == item['province_code']:
                return str(item['province']), None, None

        return None, None, None

    def query_accurate_areas(self, area_division_code):
        """
        根据纳锐人识别号, 精确 查找 开票所在的 省，市，区/县
        :param area_division_code : 行政区划代码
        :return:
        """

        for idx, item in enumerate(self.finance_records):
            if area_division_code == item['area_division_code']:
                return str(item['province']), str(item['city']), str(item['county'])

        return None, None, None


class ProvinceService:

    def __init__(self):
        sel_all_sql = "select area_id, area_name, parent_id, grade from 01_datamart_layer_007_h_cw_df.finance_province_city "
        self.province_records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_all_sql)

    def query_province_names(self, grade='1'):
        sel_sql = f'select area_name from 01_datamart_layer_007_h_cw_df.finance_province_city where grade="{grade}" '
        # print(sel_sql)
        province_names = []
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)
        for record in records:
            # print(record)
            province_names.append(record[0])

        return province_names

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
                  'operate_desc', 'unusual_infor', 'task_status']
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
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
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
