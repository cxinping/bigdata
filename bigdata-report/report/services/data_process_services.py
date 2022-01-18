# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql
from report.services.temp_api_bill_services import exec_temp_api_bill_sql_by_target
from report.commons.tools import list_of_groups
from report.commons.tools import get_current_time,get_yyyymmdd_date
from report.commons.db_helper import db_fetch_to_dict
from report.works.increment_add.exec_travel_data_gevent import check_linshi_travel_data
from report.works.increment_add.exec_offical_linshi_data import check_linshi_office_data
from report.works.increment_add.exec_meeting_linshi_data import check_linshi_meeting_data
from report.works.increment_add.exec_car_linshi_data import check_linshi_car_data
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
import time
from report.services.common_services import (insert_finance_shell_daily, update_finance_shell_daily)

log = get_logger(__name__)

"""
数据流程处理

"""


def insert_temp_performance_bill(order_number, target_classify, describe_num, sign_status, performance_sql):
    performance_id = create_uuid()
    try:
        # log.info('*** insert_finance_shell_daily ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.temp_performance_bill(performance_id, order_number, target_classify, describe_num, sign_status, performance_sql) 
        values("{performance_id}", "{order_number}", "{target_classify}", "{describe_num}","{sign_status}", "{performance_sql}" )
        """
        # log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def update_temp_performance_bill(performance_id, order_number, target_classify, describe_num, sign_status,
                                 performance_sql):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.temp_performance_bill SET order_number="{order_number}", target_classify="{target_classify}",describe_num="{describe_num}",sign_status="{sign_status}",performance_sql="{performance_sql}" WHERE performance_id="{performance_id}"
        """
        # log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def del_temp_performance_bill(performance_ids):
    try:
        sql = 'DELETE FROM 01_datamart_layer_007_h_cw_df.temp_performance_bill WHERE '
        condition_sql = ''
        in_codition = 'performance_id IN {temp}'

        if performance_ids and len(performance_ids) > 0:
            group_ls = list_of_groups(performance_ids, 1000)

            for idx, group in enumerate(group_ls):
                if len(group) == 1:
                    temp = in_codition.format(temp=str('("' + group[0] + '")'))
                else:
                    temp = in_codition.format(temp=str(tuple(group)))

                if idx == 0:
                    condition_sql = temp
                else:
                    condition_sql = condition_sql + ' OR ' + temp

            sql = sql + condition_sql
            # log.info(sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def query_temp_performance_bill(performance_ids):
    try:
        sql = 'SELECT performance_sql,order_number FROM 01_datamart_layer_007_h_cw_df.temp_performance_bill WHERE '
        condition_sql = ''
        in_codition = 'performance_id IN {temp}'

        if performance_ids and len(performance_ids) > 0:
            group_ls = list_of_groups(performance_ids, 1000)

            for idx, group in enumerate(group_ls):
                if len(group) == 1:
                    temp = in_codition.format(temp=str('("' + group[0] + '")'))
                else:
                    temp = in_codition.format(temp=str(tuple(group)))

                if idx == 0:
                    condition_sql = temp
                else:
                    condition_sql = condition_sql + ' OR ' + temp

            sql = sql + condition_sql
            # log.info(sql)
            records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
            return records
        else:
            sql = sql + ' 1=1 order by order_number asc'
            records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
            return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def query_temp_performance_bill_by_target_classify(target_classify):
    try:
        sql = f'SELECT performance_sql,order_number FROM 01_datamart_layer_007_h_cw_df.temp_performance_bill WHERE target_classify="{target_classify}" order by order_number asc'
        log.info(sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def exec_temp_performance_bill(performance_ids, is_log=True):
    try:
        daily_start_date = get_current_time()

        if is_log:
            daily_id = insert_finance_shell_daily(daily_status='ok', daily_start_date=daily_start_date,
                                                  daily_end_date='', unusual_point='',
                                                  daily_source='sql',
                                                  operate_desc=f'doing', unusual_infor='',
                                                  task_status='doing', daily_type='绩效')

        records = query_temp_performance_bill(performance_ids)
        # print(len(records), records)

        for idx, record in enumerate(records):
            performance_sql = record[0]
            # print(performance_sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=performance_sql)

        if is_log:
            operate_desc = f'成功执行绩效表中的SQL'
            daily_end_date = get_current_time()
            update_finance_shell_daily(daily_id, daily_end_date, task_status='done', operate_desc=operate_desc)
    except Exception as e:
        print(e)
        if is_log:
            error_info = str(e)
            daily_end_date = get_current_time()
            update_finance_shell_daily(daily_id, daily_end_date, task_status='error', operate_desc=error_info)

        raise RuntimeError(e)


def query_finance_data_process(query_date):
    """
    查询流程表中的数据,查询当天的每步骤最新数据
    :param query_date:
    :return:

    select cc.* from 01_datamart_layer_007_h_cw_df.finance_data_process cc,
(select distinct * from (
select
step_number,
first_value(daily_end_date) over(partition by step_number order by daily_end_date desc) max_end_date
FROM 01_datamart_layer_007_h_cw_df.finance_data_process
WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{query_date}'
AND process_status = 'sucess'
ORDER BY step_number ASC) zz) bb
where cc.step_number=bb.step_number and cc.daily_end_date=bb.max_end_date

    """
    try:
        columns_ls = ['process_id', 'process_status', 'daily_start_date', 'daily_end_date', 'step_number',
                      'operate_desc', 'orgin_source', 'destin_source', 'importdate', 'target_classify']
        columns_str = ",".join(columns_ls)
        sel_sql1 = f"select {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '20220105' AND process_status = 'sucess'  ORDER BY step_number ASC  "

        sel_sql = """
    select cc.process_id, cc.process_status, cc.daily_start_date, cc.daily_end_date, cc.step_number, cc.operate_desc, cc.orgin_source, cc.destin_source, cc.importdate, cc.target_classify from 01_datamart_layer_007_h_cw_df.finance_data_process cc,
(select distinct * from (
select 
step_number,
first_value(daily_end_date) over(partition by step_number order by daily_end_date desc) max_end_date
FROM 01_datamart_layer_007_h_cw_df.finance_data_process 
WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{query_date}' 
ORDER BY step_number ASC) zz) bb
where cc.step_number=bb.step_number and cc.daily_end_date=bb.max_end_date
        """.format(query_date=query_date)

        log.info(sel_sql)

        records = db_fetch_to_dict(sel_sql, columns_ls)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                operate_desc,
                                orgin_source, destin_source, importdate):
    """
    添加流程表的数据
    process_status 流程状态：sucess/false
    :return:
    """
    process_id = create_uuid()
    try:
        # log.info('*** insert_finance_shell_daily ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_data_process(process_id, process_status, target_classify,daily_start_date, daily_end_date, step_number,operate_desc, orgin_source, destin_source,importdate) 
        values("{process_id}", "{process_status}", "{target_classify}","{daily_start_date}","{daily_end_date}", "{step_number}", "{operate_desc}", "{orgin_source}", "{destin_source}", "{importdate}" )
        """.replace('\n', '').replace('\r', '').strip()
        # log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return process_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def pagination_temp_performance_bill_records():
    """
    分页查询绩效临时表的记录

    :return:
    """
    columns_ls = ['performance_id', 'order_number', 'target_classify', 'describe_num', 'sign_status', 'performance_sql']
    columns_str = ",".join(columns_ls)
    sql = f"SELECT {columns_str} FROM 01_datamart_layer_007_h_cw_df.temp_performance_bill "

    count_sql = 'SELECT count(1) FROM ({sql}) a'.format(sql=sql)
    log.info(count_sql)
    records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=count_sql)
    count_records = records[0][0]
    # print('* count_records => ', count_records)

    ###### 拼装查询SQL
    where_sql = 'WHERE '
    condition_sql = ''

    where_sql = where_sql + ' 1=1 '

    order_sql = ' ORDER BY order_number ASC '
    sql = sql + where_sql + order_sql

    return count_records, sql, columns_ls


def query_temp_receipt_address():
    """
    查询发票地址sql表
    :param query_date:
    :return:
    """
    try:
        sql = 'SELECT receipt_sql,receipt_id FROM 01_datamart_layer_007_h_cw_df.temp_receipt_address ORDER BY receipt_id ASC'
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def query_temp_receipt_address_by_target_classify(target_classify):
    """
    根据目标分类查询发票地址sql表
    :param query_date:
    :return:
    """
    try:
        sql = f'SELECT receipt_sql,receipt_id FROM 01_datamart_layer_007_h_cw_df.temp_receipt_address WHERE target_classify="{target_classify}" ORDER BY receipt_id ASC'
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def query_finance_unusual(cost_project=None):
    """
    查询差旅费异常明细表
    :return:
    """
    try:
        condition = None
        if cost_project is not None:
            condition = f' cost_project="{cost_project}" '
        else:
            condition = ' 1=1 '

        sql = f'SELECT unusual_shell,isalgorithm,unusual_id FROM 01_datamart_layer_007_h_cw_df.finance_unusual WHERE {condition} AND sign_status="1" ORDER BY unusual_id ASC'
        # log.info(sql)
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


class BaseProcess(metaclass=ABCMeta):

    def __init__(self):
        pass

    @abstractmethod
    def exec_steps(self, data_date):
        pass

    def exec_step06(self):
        """
        执行第6步
        6、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
        :return:
        """

        travel_fee = '差旅费'
        meeting_fee = '会议费'
        office_fee = '办公费'
        car_fee = '车辆使用费'

        travel_records = query_temp_receipt_address_by_target_classify(travel_fee)
        meeting_records = query_temp_receipt_address_by_target_classify(meeting_fee)
        office_records = query_temp_receipt_address_by_target_classify(office_fee)
        car_records = query_temp_receipt_address_by_target_classify(car_fee)

        threadPool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="thr_add")
        all_task = []
        task1 = threadPool.submit(self.__exec_step06_task, travel_records, travel_fee)
        task2 = threadPool.submit(self.__exec_step06_task, meeting_records, meeting_fee)
        task3 = threadPool.submit(self.__exec_step06_task, office_records, office_fee)
        task4 = threadPool.submit(self.__exec_step06_task, car_records, car_fee)
        all_task.append(task1)
        all_task.append(task2)
        all_task.append(task3)
        all_task.append(task4)

        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

    def __exec_step06_task(self, records, target_classify):
        try:
            daily_start_date = get_current_time()
            for idx, record in enumerate(records):
                sql = str(record[0])
                receipt_id = str(record[1])

                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
                log.info(f'* 第6步，成功执行 target_classify为 {target_classify},receipt_id为 {receipt_id} 的SQL')

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = f'成功执行目标分类为{target_classify}的SQL'
            orgin_source = '发票信息hive表'
            destin_source = 'kudu分析表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 执行第6步，目标分类为{target_classify} ，序号为{receipt_id}的SQL报错')
            log.info(sql)
            print(e)

            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = str(e)
            orgin_source = '发票信息hive表'
            destin_source = 'kudu分析表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
            # raise RuntimeError(e)

    def exec_step07(self):
        """
        执行第7步
        7、稽查点sql将数据写到kudu落地表（脚本）
        :return:
        """

        travel_fee = '差旅费'
        meeting_fee = '会议费'
        office_fee = '办公费'
        car_fee = '车辆使用费'

        travel_records = query_finance_unusual(cost_project=travel_fee)
        meeting_records = query_finance_unusual(cost_project=meeting_fee)
        office_records = query_finance_unusual(cost_project=office_fee)
        car_records = query_finance_unusual(cost_project=car_fee)
        threadPool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="thr")
        all_task = []
        task1 = threadPool.submit(self.__exec_step07_task, travel_records, travel_fee)
        task2 = threadPool.submit(self.__exec_step07_task, meeting_records, meeting_fee)
        task3 = threadPool.submit(self.__exec_step07_task, office_records, office_fee)
        task4 = threadPool.submit(self.__exec_step07_task, car_records, car_fee)
        all_task.append(task1)
        all_task.append(task2)
        all_task.append(task3)
        all_task.append(task4)

        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

    def __exec_step07_task(self, records, cost_project):
        daily_start_date = get_current_time()

        for record in records:
            unusual_shell = str(record[0])
            isalgorithm = str(record[1])
            unusual_id = str(record[2])

            try:
                # time.sleep(1)

                if isalgorithm == '1':
                    ### 执行 SQL ###
                    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
                    log.info(f'* 第7步，成功执行 cost_project为 {cost_project}, unusual_id为 {unusual_id} 的SQL')
                elif isalgorithm == '2':
                    ### 执行算法 python 脚本  ###
                    exec(unusual_shell, globals())
                    log.info(f'* 第7步，成功执行 cost_project为 {cost_project}, unusual_id为 {unusual_id} 的Python Shell')

                process_status = 'sucess'
                daily_end_date = get_current_time()
                step_number = '7'
                operate_desc = f'成功执行目标分类为 {cost_project} 的SQL'
                orgin_source = 'kudu分析表'
                destin_source = 'kudu落地表'
                importdate = get_yyyymmdd_date()
                insert_finance_data_process(process_status, cost_project, daily_start_date, daily_end_date,
                                            step_number, operate_desc,
                                            orgin_source, destin_source, importdate)
            except Exception as e:
                daily_source = 'SQL' if isalgorithm == '1' else 'Python Shell'
                log.error(f'* 执行第7步，目标分类为 {cost_project} ,unusual_id为 {unusual_id} 的 {daily_source} 报错')
                print(e)
                process_status = 'false'
                daily_end_date = get_current_time()
                step_number = '7'
                operate_desc = str(e)
                orgin_source = 'kudu分析表'
                destin_source = 'kudu落地表'
                importdate = get_yyyymmdd_date()
                insert_finance_data_process(process_status, cost_project, daily_start_date, daily_end_date,
                                            step_number, operate_desc,
                                            orgin_source, destin_source, importdate)
                # raise RuntimeError(e)

    def exec_step09(self):
        """
        执行第9步
        9、差旅、会议、办公、车辆费聚合接口API（脚本）
        :return:
        """
        travel_fee = '差旅费'
        meeting_fee = '会议费'
        office_fee = '办公费'
        car_fee = '车辆使用费'

        threadPool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="thr")
        all_task = []
        task1 = threadPool.submit(self.__exec_step09_task, travel_fee)
        task2 = threadPool.submit(self.__exec_step09_task, meeting_fee)
        task3 = threadPool.submit(self.__exec_step09_task, office_fee)
        task4 = threadPool.submit(self.__exec_step09_task, car_fee)
        all_task.append(task1)
        all_task.append(task2)
        all_task.append(task3)
        all_task.append(task4)

        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

    def __exec_step09_task(self, target_classify):
        try:
            daily_start_date = get_current_time()
            exec_temp_api_bill_sql_by_target(target_classify=target_classify, is_log=False)

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '9'
            operate_desc = f'成功执行聚合临时表API表的目标分类为{target_classify}的SQL'
            orgin_source = 'kudu分析表/落地表'
            destin_source = '汇总API中间表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 第9步，执行SQL报错')
            print(e)
            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '9'
            operate_desc = ''
            orgin_source = 'kudu分析表/落地表'
            destin_source = '汇总API中间表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
            # raise RuntimeError(e)

    def exec_step08(self):
        """
        执行第8步：
        8、绩效接口API（脚本）
        :return:
        """

        # 清空落地表数据
        # sql = 'delete from 01_datamart_layer_007_h_cw_df.finance_performance_api'
        # log.info('* 第8步，开始清空落地表数据')
        # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        # log.info('* 第8步，成功清空落地表数据 ==> 01_datamart_layer_007_h_cw_df.finance_performance_api')

        travel_fee = '差旅费'
        meeting_fee = '会议费'
        office_fee = '办公费'
        car_fee = '车辆使用费'

        travel_records = query_temp_performance_bill_by_target_classify(travel_fee)
        meeting_records = query_temp_performance_bill_by_target_classify(meeting_fee)
        office_records = query_temp_performance_bill_by_target_classify(office_fee)
        car_records = query_temp_performance_bill_by_target_classify(car_fee)

        threadPool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="thr")
        all_task = []
        task1 = threadPool.submit(self.__exec_step08_task, travel_records, travel_fee)
        task2 = threadPool.submit(self.__exec_step08_task, meeting_records, meeting_fee)
        task3 = threadPool.submit(self.__exec_step08_task, office_records, office_fee)
        task4 = threadPool.submit(self.__exec_step08_task, car_records, car_fee)
        all_task.append(task1)
        all_task.append(task2)
        all_task.append(task3)
        all_task.append(task4)

        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

    def __exec_step08_task(self, records, target_classify):
        daily_start_date = get_current_time()

        try:
            for record in records:
                sql = str(record[0])
                order_number = str(record[1])

                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
                log.info(f'* 第8步，成功执行序号为 {order_number} ,target_classify为 {target_classify} 的SQL')

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '8'
            operate_desc = f'成功执行绩效接口API表的目标分类为{target_classify}的SQL'
            orgin_source = 'kudu分析表/落地表'
            destin_source = '绩效API中间表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 执行第8步，序号为 {order_number} 的SQL报错,target_classify为 {target_classify} 的SQL')
            # print(sql)
            print(e)
            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '8'
            operate_desc = str(e)
            orgin_source = 'kudu分析表/落地表'
            destin_source = '绩效API中间表'
            importdate = get_yyyymmdd_date()
            insert_finance_data_process(process_status, target_classify, daily_start_date, daily_end_date, step_number,
                                        operate_desc,
                                        orgin_source, destin_source, importdate)
            # raise RuntimeError(e)


class FullAddProcess(BaseProcess):
    """ 全量数据流程 """

    def __init__(self):
        pass

    def exec_step06(self):
        log.info('执行第6步，全量数据流程')
        super().exec_step06()

    def exec_step07(self):
        log.info('执行第7步，全量数据流程')

        try:
            # 删除结果表中的数据
            sql = 'delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets'
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            log.info('全量数据流程执行第7步，首先删除结果表中的数据')

            super().exec_step07()
        except Exception as e:
            # log.error(f'* 执行第7步的SQL或Python Shell报错')
            # print(e)
            raise RuntimeError(e)

    def exec_step08(self):
        log.info('执行第8步，全量数据流程')
        super().exec_step08()

    def exec_step09(self):
        log.info('执行第9步，全量数据流程')
        super().exec_step09()

    def exec_steps(self):
        """
        执行步骤 6,7,8,9
        :return:
        """

        # self.exec_step06()

        # self.exec_step07()

        # self.exec_step08()

        self.exec_step09()


class IncrementAddProcess(BaseProcess):
    """ 增量数据流程 """

    def __init__(self):
        pass

    def exec_linshi_daily_data(self):
        """
        增量更新临时表近两个月的数据
        :return:
        """
        log.info('***** 在执行第5步前，增量数据流程，4个费用的前两个月数据入库 *****')
        check_linshi_travel_data()
        check_linshi_office_data()
        check_linshi_meeting_data()
        check_linshi_car_data()

    def exec_step06(self):
        """
        执行第五步
        6、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
        :return:
        """
        log.info("*" * 30)
        log.info('***** 执行第6步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step06()

    def exec_step07(self):
        """
        执行第六步
        7、稽查点sql将数据写到kudu落地表（脚本）
        :return:
        """
        log.info("*" * 30)
        log.info('***** 执行第7步，增量数据流程 *****')
        log.info("*" * 30)

        try:
            # 删除结果表中的数据
            sql = 'delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets'
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            log.info('增量数据流程执行第7步，首先删除结果表中的数据')

            super().exec_step07()
        except Exception as e:
            # log.error(f'* 执行第7步的SQL或Python Shell报错')
            # print(e)
            raise RuntimeError(e)

    def exec_step08(self):
        log.info("*" * 30)
        log.info('***** 执行第8步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step08()

    def exec_step09(self):
        log.info("*" * 30)
        log.info('***** 执行第9步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step09()

    def exec_steps(self):
        """
        执行步骤 6,7,8,9
        :return:
        """
        self.exec_linshi_daily_data()

        self.exec_step06()

        self.exec_step07()

        self.exec_step08()

        self.exec_step09()


if __name__ == '__main__':
    # full_process = FullAddProcess()
    # full_process.exec_steps()

    increment_process = IncrementAddProcess()
    increment_process.exec_steps()

    print('--- ok，done ---')
