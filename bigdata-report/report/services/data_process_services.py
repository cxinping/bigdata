# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql
from report.services.temp_api_bill_services import exec_temp_api_bill_sql_by_target
from report.commons.tools import list_of_groups
from report.commons.runengine import execute_kudu_sql, execute_py_shell
from report.commons.tools import get_current_time
from report.commons.db_helper import db_fetch_to_dict
from report.works.increment_add.exec_travel_data_gevent import check_linshi_travel_data
from report.works.increment_add.exec_offical_linshi_data import check_linshi_office_data
from report.works.increment_add.exec_meeting_linshi_data import check_linshi_meeting_data
from report.works.increment_add.exec_car_linshi_data import check_linshi_car_data
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED

log = get_logger(__name__)

"""
数据流程处理

"""


def insert_temp_performance_bill(order_number, describe_num, sign_status, performance_sql):
    performance_id = create_uuid()
    try:
        # log.info('*** insert_finance_shell_daily ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.temp_performance_bill(performance_id, order_number, describe_num, sign_status, performance_sql) 
        values("{performance_id}", "{order_number}", "{describe_num}","{sign_status}", "{performance_sql}" )
        """  # .replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def update_temp_performance_bill(performance_id, order_number, describe_num, sign_status, performance_sql):
    try:
        sql = f"""
        UPDATE 01_datamart_layer_007_h_cw_df.temp_performance_bill SET order_number="{order_number}", describe_num="{describe_num}",sign_status="{sign_status}",performance_sql="{performance_sql}" WHERE performance_id="{performance_id}"
        """  # .replace('\n', '').replace('\r', '').strip()
        log.info(sql)
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


def exec_temp_performance_bill(performance_ids):
    try:
        records = query_temp_performance_bill(performance_ids)
        # print(len(records), records)

        for idx, record in enumerate(records):
            performance_sql = record[0]
            # print(performance_sql)
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=performance_sql)
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def query_finance_data_process(query_date):
    """
    查询流程表中的数据,查询当天的每步骤最新数据
    :param query_date:
    :return:
    """
    try:
        columns_ls = ['process_id', 'process_status', 'daily_start_date', 'daily_end_date', 'step_number',
                      'operate_desc', 'orgin_source', 'destin_source', 'importdate']
        columns_str = ",".join(columns_ls)

        sel_sql1 = f"select {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '20220105' AND process_status = 'sucess'  ORDER BY step_number ASC  "
        sel_sql = """
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
        """.format(query_date=query_date)

        # log.info(sel_sql)

        records = db_fetch_to_dict(sel_sql, columns_ls)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
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
        insert into 01_datamart_layer_007_h_cw_df.finance_data_process(process_id, process_status, daily_start_date, daily_end_date, step_number,operate_desc, orgin_source, destin_source,importdate) 
        values("{process_id}", "{process_status}", "{daily_start_date}","{daily_end_date}", "{step_number}", "{operate_desc}", "{orgin_source}", "{destin_source}", "{importdate}" )
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
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
    columns_ls = ['performance_id', 'order_number', 'describe_num', 'sign_status', 'performance_sql']
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


def query_finance_unusual():
    """
    查询差旅费异常明细表
    :return:
    """
    try:
        sql = 'SELECT unusual_shell,isalgorithm,unusual_id FROM 01_datamart_layer_007_h_cw_df.finance_unusual WHERE sign_status="1" ORDER BY unusual_id ASC'
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

    def exec_step05(self):
        """
        执行第五步
        5、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
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

        threadPool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="thr")
        all_task = []
        task1 = threadPool.submit(self.__exec_step05_task, travel_records, travel_fee)
        task2 = threadPool.submit(self.__exec_step05_task, meeting_records, meeting_fee)
        task3 = threadPool.submit(self.__exec_step05_task, office_records, office_fee)
        task4 = threadPool.submit(self.__exec_step05_task, car_records, car_fee)
        all_task.append(task1)
        all_task.append(task2)
        all_task.append(task3)
        all_task.append(task4)

        wait(all_task, return_when=ALL_COMPLETED)
        threadPool.shutdown(wait=True)

    def __exec_step05_task(self, records, target_classify):
        try:
            daily_start_date = get_current_time()
            for idx, record in enumerate(records):
                sql = str(record[0])
                receipt_id = str(record[1])

                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
                log.info(f'* 第5步，成功执行 target_classify为 {target_classify},receipt_id为 {receipt_id} 的SQL')

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '5'
            operate_desc = f'成功执行目标分类为{target_classify}的SQL'
            orgin_source = '发票信息hive表'
            destin_source = 'kudu分析表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 执行第5步，目标分类为{target_classify} ，序号为{receipt_id}的SQL报错')
            log.info(sql)
            print(e)

            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '5'
            operate_desc = str(e)
            orgin_source = '发票信息hive表'
            destin_source = 'kudu分析表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
            raise RuntimeError(e)

    def exec_step06(self):
        """
        执行第七步
        6、稽查点sql将数据写到kudu落地表（脚本）
        :return:
        """
        records = query_finance_unusual()
        for record in records:
            unusual_shell = str(record[0])
            isalgorithm = str(record[1])
            unusual_id = str(record[2])

            try:
                if isalgorithm == '1':
                    ### 执行 SQL ###
                    prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=unusual_shell)
                    log.info(f'* 第6步，成功执行unusual_id为 {unusual_id} 的SQL')
                elif isalgorithm == '2':
                    ### 执行算法 python 脚本  ###
                    exec(unusual_shell, globals())
                    log.info(f'* 第6步，成功执行unusual_id为 {unusual_id} 的Python Shell')
            except Exception as e:
                daily_source = 'SQL' if isalgorithm == '1' else 'Python Shell'
                log.error(f'* 执行第6步，unusual_id为{unusual_id}的{daily_source}报错')
                # raise RuntimeError(e)
                print(e)

    def exec_step07(self):
        """
        执行第七步
        7、差旅、会议、办公、车辆费聚合接口API（脚本）
        :return:
        """

        try:
            daily_start_date = get_current_time()
            target_classify_ls = ['差旅费', '会议费', '办公费', '车辆使用费']
            for item in target_classify_ls:
                exec_temp_api_bill_sql_by_target(item)

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '7'
            operate_desc = ''
            orgin_source = 'kudu分析表/落地表'
            destin_source = '汇总API中间表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 第7步，执行SQL报错')
            print(e)
            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '7'
            operate_desc = ''
            orgin_source = 'kudu分析表/落地表'
            destin_source = '汇总API中间表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)

    def exec_step08(self):
        """
        执行第八步：
        8、绩效接口API（脚本）
        :return:
        """
        daily_start_date = get_current_time()
        # 清空落地表数据
        sql = 'delete from 01_datamart_layer_007_h_cw_df.finance_performance_api'
        log.info('* 第8步，开始清空落地表数据')
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        log.info('* 第8步，成功清空落地表数据 ==> 01_datamart_layer_007_h_cw_df.finance_performance_api')
        try:
            records = query_temp_performance_bill(None)

            for record in records:
                sql = str(record[0])
                order_number = str(record[1])

                # if order_number in ['05', '06']:
                #     continue

                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
                log.info(f'* 第8步，成功执行序号为 {order_number} 的SQL')

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '8'
            operate_desc = ''
            orgin_source = 'kudu分析表/落地表'
            destin_source = '绩效API中间表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            log.error(f'* 执行第8步，序号为{order_number}的SQL报错')
            # print(sql)
            print(e)

            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '8'
            operate_desc = str(e)
            orgin_source = 'kudu分析表/落地表'
            destin_source = '绩效API中间表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
            # raise RuntimeError(e)


class FullAddProcess(BaseProcess):
    """ 全量数据流程 """

    def __init__(self):
        pass

    def exec_step05(self):
        log.info('执行第5步，全量数据流程')
        super().exec_step05()

    def exec_step06(self):
        log.info('执行第6步，全量数据流程')
        daily_start_date = get_current_time()
        # 删除结果表中的数据
        sql = 'delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets'
        try:
            prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            log.info('全量数据流程执行第6步，首先删除结果表中的数据')

            super().exec_step06()

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = ''
            orgin_source = 'kudu分析表'
            destin_source = 'kudu落地表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            print(e)
            # log.error(f'* 执行第6步的SQL或Python Shell报错')

            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = str(e)
            orgin_source = 'kudu分析表'
            destin_source = 'kudu落地表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
            raise RuntimeError(e)

    def exec_step07(self):
        log.info('执行第7步，全量数据流程')
        super().exec_step07()

    def exec_step08(self):
        log.info('执行第8步，全量数据流程')
        super().exec_step08()

    def exec_steps(self):
        """
        执行步骤 5,6,7,8
        :return:
        """

        # self.exec_step05()

        # self.exec_step06()

        # self.exec_step07()

        self.exec_step08()


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
        # check_linshi_office_data()
        # check_linshi_meeting_data()
        # check_linshi_car_data()

    def exec_step05(self):
        """
        执行第五步
        5、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
        :return:
        """
        log.info("*" * 30)
        log.info('***** 执行第5步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step05()

    def exec_step06(self):
        """
        执行第六步
        6、稽查点sql将数据写到kudu落地表（脚本）
        :return:
        """
        log.info("*" * 30)
        log.info('***** 执行第6步，增量数据流程 *****')
        log.info("*" * 30)

        daily_start_date = get_current_time()
        # 删除结果表中的数据
        # sql = 'delete from analytic_layer_zbyy_cwyy_014_cwzbbg.finance_all_targets'
        try:
            # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            # log.info('增量数据流程执行第6步，首先删除结果表中的数据')

            super().exec_step06()

            process_status = 'sucess'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = ''
            orgin_source = 'kudu分析表'
            destin_source = 'kudu落地表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
        except Exception as e:
            # log.error(f'* 执行第6步的SQL或Python Shell报错')
            print(e)
            process_status = 'false'
            daily_end_date = get_current_time()
            step_number = '6'
            operate_desc = str(e)
            orgin_source = 'kudu分析表'
            destin_source = 'kudu落地表'
            importdate = get_current_time()
            insert_finance_data_process(process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                        orgin_source, destin_source, importdate)
            raise RuntimeError(e)

    def exec_step07(self):
        log.info("*" * 30)
        log.info('***** 执行第7步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step07()

    def exec_step08(self):
        log.info("*" * 30)
        log.info('***** 执行第8步，增量数据流程 *****')
        log.info("*" * 30)

        super().exec_step08()

    def exec_steps(self):
        """
        执行步骤 5,6,7,8
        :return:
        """
        # self.exec_linshi_daily_data()

        self.exec_step05()

        # self.exec_step06()
        #
        # self.exec_step07()
        #
        # self.exec_step08()


if __name__ == '__main__':
    # full_process = FullAddProcess()
    # full_process.exec_steps()

    increment_process = IncrementAddProcess()
    increment_process.exec_steps()

    print('--- ok，done ---')
