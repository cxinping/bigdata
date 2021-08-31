# -*- coding: utf-8 -*-
import asyncio, aiomysql
import time
from report.commons.logging import get_logger
import sys

sys.path.append("/usr/local/lib64/python3.6/site-packages")

import jaydebeapi
import jpype
import os
import traceback
import pandas as pd
import time
from report.commons.connect_kudu import prod_execute_sql, dis_connection

log = get_logger(__name__)


class AsyncKudu(object):
    def __init__(self, loop):
        self._loop = loop

    '''外面调用这个方法，传sql列表一次500-1000个sql即可'''

    async def exe_sql_task(self, sqltype='insert', sql_list=[]):
        if isinstance(sql_list, list):
            if sqltype == 'insert':
                task_list = [self.exe_sql(sql=sql) for sql in sql_list]
            elif sqltype == 'select':
                task_list = [self.exe_sql(sqltype=sqltype, sql=sql) for sql in sql_list]
            sql_list = None
            if len(task_list) > 0:
                log.info('start execute sql')
                result, pending = await asyncio.wait(task_list)
                log.info('execute sql down')
                if pending:
                    log.info('canceling tasks')
                    log.error(pending)
                    for t in pending:
                        t.cancel()
                if result:
                    return result
                else:
                    return None
            else:
                log.info('Error constructing SQL list')
        else:
            log.error('exe_sql_task方法传入的sql与param应为list')

    async def exe_pro_sql(self, sqltype='insert', sql=''):
        is_commit = False
        conn = None
        database_list = None
        jars_path = '/you_filed_algos/jars/'
        dirver = "org.apache.hive.jdbc.HiveDriver"
        is_prod_env = True

        if is_prod_env:
            url = "jdbc:hive2://hadoop-pro-017:7180/default;ssl=true;sslTrustStore=/you_filed_algos/prod-cm-auto-global_truststore.jks;principal=impala/hadoop-pro-017@BYHW.HADOOP.COM"
        else:
            url = "jdbc:hive2://bigdata-dev-014:7180/;ssl=true;sslTrustStore=/you_filed_algos/cm-auto-global_truststore.jks;principal=impala/bigdata-dev-014@SJFWPT.SINOPEC.COM"

        jars_file_ls = []
        jars_file_str = ''
        for jar in os.listdir(jars_path):
            jars_file_ls.append(jars_path + jar)

        jars_file_str = ':'.join(jars_file_ls)

        jvm_options = "-Djava.class.path=" + jars_file_str

        # jvm = jpype.getDefaultJVMPath()
        jvm = '/you_filed_algos/jdk8/jre/lib/amd64/server/libjvm.so'

        if not jpype.isJVMStarted():
            try:
                # print('--------startjvm---------')
                jpype.startJVM(jvm, jvm_options)
                # print("JVM path:"+ jpype.getDefaultJVMPath())
                # print('----- running jvm -------------')

            except Exception as e:
                print('====== throw error ======')
                traceback.print_exc()
                jpype.shutdownJVM()

        System = jpype.java.lang.System

        if is_prod_env:
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/prod-krb5.conf")
        else:
            System.setProperty("java.security.krb5.conf", "/you_filed_algos/krb5.conf")

        Configuration = jpype.JPackage('org.apache.hadoop.conf').Configuration
        conf = Configuration()
        conf.set("hadoop.security.authentication", "kerberos")

        try:
            UserGroupInformation = jpype.JClass('org.apache.hadoop.security.UserGroupInformation')
            UserGroupInformation.setConfiguration(conf)

            if is_prod_env:
                UserGroupInformation.loginUserFromKeytab("sjfw_wangsh12348", "/you_filed_algos/sjfw_wangsh12348.keytab")
            else:
                UserGroupInformation.loginUserFromKeytab("sjfw_pbpang", "/you_filed_algos/sjfw_pbpang.keytab")

            conn = jaydebeapi.connect(dirver, url)
            cur = conn.cursor()
            result = None
            if sqltype == 'insert':
                cur.execute(sql)
            else:
                cur.execute(sql)
                result = cur.fetchall()

            # 关闭游标
            cur.close()
            # 关闭连接
            conn.close()
            if sqltype != 'insert':
                return result

            # async with jaydebeapi.connect(dirver, url) as conn:
            #     async with conn.cursor() as cur:
            #         result = None
            #
            #         if sqltype == 'insert':
            #             await cur.execute(sql)
            #         else:
            #             await cur.execute(sql)
            #             result = await cur.fetchall()
            #
            #         # 关闭游标
            #         await cur.close()
            #         # 关闭连接
            #         await conn.close()
            #         if sqltype != 'insert':
            #             return result
        except Exception as ex:
            print(ex)
            traceback.print_exc()

    async def exe_sql(self, sqltype='insert', sql=''):
        result = None
        try:
            if sqltype == 'insert':
                await self.exe_pro_sql(sqltype=sqltype, sql=sql)
            elif sqltype == 'select':
                result = await self.exe_pro_sql(sqltype=sqltype, sql=sql)
        except Exception as ex:
            ''' 新增代码将错误记录写入数据库，唯一ID、sql、异常信息三列即可 '''
            log.info('SQL：{}\n 执行异常，错误原因为：'.format(sql))
            log.error(ex)
            return None
        return result

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


async def exec_insert(event_loop, sqltype='insert', sqllist=[]):
    async with AsyncKudu(event_loop) as ae:
        results = await ae.exe_sql_task(sqltype, sqllist)
    return results


def demo1():
    from datetime import datetime
    columns_ls = ['company_code', 'bill_id', 'account_period', 'account_item', 'finance_number', 'cost_center',
                  'profit_center', 'bill_code', 'origin_name', 'destin_name', 'travel_beg_date', 'travel_end_date',
                  'jour_amount', 'accomm_amount', 'subsidy_amount', 'other_amount',
                  'apply_emp_id', 'apply_emp_name', 'check_amount', 'jzpz']
    columns_str = ",".join(columns_ls)

    # part1 查询异常数据的count
    x = datetime.now()
    sql = 'select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill where check_amount > jzpz limit 1000'.format(
        columns_str=columns_str)
    count_sql = 'select count(a.bill_id) from ({sql}) a'.format(sql=sql)

    records = prod_execute_sql(sqltype='select', sql=count_sql)
    count_records = records[0][0]
    max_size = 1 * 1000
    limit_size = 100
    select_sql_ls = []

    log.info('* count_records={count_records}'.format(count_records=count_records))
    if count_records >= max_size:
        offset_size = 0
        while offset_size <= count_records:

            if offset_size + limit_size > count_records:
                limit_size = count_records - offset_size
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)

                select_sql_ls.append(tmp_sql)
                break
            else:
                tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill order by finance_travel_id limit {limit_size} offset {offset_size}".format(
                    columns_str=columns_str, limit_size=limit_size, offset_size=offset_size)
                select_sql_ls.append(tmp_sql)

            offset_size = offset_size + limit_size
    else:
        tmp_sql = "select {columns_str} from 01_datamart_layer_007_h_cw_df.finance_travel_bill".format(
            columns_str=columns_str)
        select_sql_ls.append(tmp_sql)

    print(len(select_sql_ls), select_sql_ls)
    # for sql in select_sql_ls:
    #     print(sql)

    # part2  insert sql
    event_loop = asyncio.new_event_loop()
    task = event_loop.create_task(exec_insert(event_loop, sqltype='select', sqllist=select_sql_ls))
    event_loop.run_until_complete(task)
    event_loop.close()
    log.info('查询共耗时' + str(datetime.now() - x))

    results = task.result()
    insert_sql_ls = []
    if results:
        for rs in results:
            if rs.result():
                for item in rs.result():
                    # log.info(item)
                    company_code = item[0] if item[0] is not None else ''
                    bill_id = item[1] if item[1] is not None else ''
                    account_period = item[2] if item[2] is not None else ''
                    account_item = item[3] if item[3] is not None else ''
                    finance_number = item[4] if item[4] is not None else ''
                    cost_center = item[5] if item[5] is not None else ''
                    profit_center = item[6] if item[6] is not None else ''
                    cart_head = ''
                    bill_code = item[7] if item[7] is not None else ''
                    origin_city = item[8] if item[8] is not None else ''
                    destin_city = item[9] if item[9] is not None else ''
                    beg_date = item[10] if item[10] is not None else ''
                    end_date = item[11] if item[11] is not None else ''
                    jour_amount = item[12] if item[12] is not None else 0
                    accomm_amount = item[13] if item[13] is not None else 0

                    subsidy_amount = item[14] if item[14] is not None else 0
                    other_amount = item[15] if item[15] is not None else 0

                    emp_code = item[16] if item[16] is not None else ''
                    emp_name = item[17] if item[17] is not None else ''
                    check_amount = item[18]
                    jzpz = item[19]
                    unusual_id = '03'

                    insert_sql = """
                    insert into table 01_datamart_layer_007_h_cw_df.finance_all_targets(company_code, bill_id, account_period, 
                            account_item , finance_number ,cost_center, 
                            profit_center, cart_head, bill_code, 
                            origin_city, destin_city, beg_date, end_date,
                            jour_amount, accomm_amount, 
                            subsidy_amount, other_amount,
                            emp_code,emp_name, 
                            check_amount,  jzpz, unusual_id) 
                            values( "{company_code}", "{bill_id}" ,  "{account_period}" , 
                            "{account_item}" ,"{finance_number}", "{cost_center}", 
                            "{profit_center}", "{cart_head}", "{bill_code}" , 
                            "{origin_city}", "{destin_city}", "{beg_date}", "{end_date}",                            
                            {jour_amount}, {accomm_amount},    
                            {other_amount}, {other_amount},                        
                            "{emp_code}", "{emp_name}" , 
                            {check_amount} , {jzpz} , "{unusual_id}" )
                            """.format(company_code=company_code, bill_id=bill_id,
                                       account_period=account_period, account_item=account_item,
                                       finance_number=finance_number, cost_center=cost_center,
                                       profit_center=profit_center, cart_head=cart_head, bill_code=bill_code,
                                       origin_city=origin_city, destin_city=destin_city, beg_date=beg_date,
                                       end_date=end_date,
                                       jour_amount=jour_amount, accomm_amount=accomm_amount,
                                       subsidy_amount=subsidy_amount, other_amount=other_amount,
                                       emp_code=emp_code, emp_name=emp_name,
                                       check_amount=check_amount, jzpz=jzpz, unusual_id=unusual_id)
                    insert_sql_ls.append(insert_sql)
                    # print(insert_sql)

    x = datetime.now()
    event_loop = asyncio.new_event_loop()
    event_loop.run_until_complete(exec_insert(event_loop, sqltype='insert', sqllist=insert_sql_ls))
    event_loop.close()
    log.info('单次插入' + str(len(insert_sql_ls)) + '条记录，' + '共耗时' + str(datetime.now() - x))
    dis_connection()


if __name__ == "__main__":
    demo1()








