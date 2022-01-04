# -*- coding: utf-8 -*-
from abc import ABCMeta, abstractmethod
from report.commons.tools import create_uuid
from report.commons.settings import CONN_TYPE
from report.commons.logging import get_logger
from report.commons.connect_kudu2 import prod_execute_sql
from report.services.temp_api_bill_services import exec_temp_api_bill_sql
from report.commons.tools import list_of_groups
from report.works.increment_add.exec_travel_data_gevent import check_linshi_travel_data
from report.works.increment_add.exec_offical_linshi_data import check_linshi_office_data
from report.works.increment_add.exec_meeting_linshi_data import check_linshi_meeting_data
from report.works.increment_add.exec_car_linshi_data import check_linshi_car_data

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
        """.replace('\n', '').replace('\r', '').strip()
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
        """.replace('\n', '').replace('\r', '').strip()
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


def query_finance_data_process(query_date):
    """
    查询流程表中的数据
    :param query_date:
    :return:
    """
    try:
        sql = f'SELECT performance_sql,order_number FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE daily_start_date > "{query_date}" AND  daily_end_date < "{query_date}" '
        records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sql)
        return records
    except Exception as e:
        print(e)
        raise RuntimeError(e)


def insert_finance_data_process(process_id, process_status, daily_start_date, daily_end_date, step_number, operate_desc,
                                orgin_source, destin_source, importdate):
    """
    添加流程表的数据
    :return:
    """
    performance_id = create_uuid()
    try:
        # log.info('*** insert_finance_shell_daily ***')
        sql = f"""
        insert into 01_datamart_layer_007_h_cw_df.finance_data_process(process_id, process_status, daily_start_date, daily_end_date, step_number,operate_desc, orgin_source, destin_source,importdate) 
        values("{process_id}", "{process_status}", "{daily_start_date}","{daily_end_date}", "{step_number}", "{operate_desc}", "{orgin_source}", "{destin_source}", "{importdate}" )
        """.replace('\n', '').replace('\r', '').strip()
        log.info(sql)
        prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
        return performance_id
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


class BaseProcess(metaclass=ABCMeta):

    def __init__(self):
        pass

    # @abstractmethod
    # def step001(self, data_date):
    #     pass

    def exec_step05(self):
        """
        执行第五步
        5、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
        :return:
        """
        pass

    def exec_step07(self):
        """
        执行第七步
        7、差旅、会议、办公、车辆费聚合接口API（脚本）
        :return:
        """
        target_classify_ls = ['差旅费', '会议费', '办公费', '车辆使用费']
        for item in target_classify_ls:
            exec_temp_api_bill_sql(item)

    def exec_step08(self):
        """
        执行第八步：
        8、绩效接口API（脚本）
        :return:
        """

        # 清空落地表数据
        sql = 'delete from 01_datamart_layer_007_h_cw_df.finance_performance_api'
        # prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)

        records = query_temp_performance_bill(None)

        for record in records:
            sql = record[0]
            order_number = record[1]
            log.info(f'* 开始执行第八步，序号为{order_number}的SQL')
            # print(sql)
            # print()
            try:
                prod_execute_sql(conn_type=CONN_TYPE, sqltype='insert', sql=sql)
            except Exception as e:
                print(e)
                log.error(f'* 执行第八步，序号为{order_number}的SQL报错')
                raise RuntimeError(e)


class FullAddProcess(BaseProcess):
    """ 全量数据流程 """
    pass


class IncrementAddProcess(BaseProcess):
    """ 增量数据流程 """

    def exec_step05(self):
        """
        执行第五步
        5、发票地址hive数据更新到kudu分析表（初始化/增量脚本）
        :return:
        """
        check_linshi_travel_data()
        # check_linshi_office_data()
        # check_linshi_meeting_data()
        # check_linshi_car_data()

    def exec_step06(self):
        """
        执行第六步
        6、稽查点sql将数据写到kudu落地表（脚本）
        :return:
        """
        pass
