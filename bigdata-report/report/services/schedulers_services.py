# -*- coding: utf-8 -*-

from report.commons.logging import get_logger
import threading
import time
from report.commons.tools import get_current_time
from report.services.data_process_services import IncrementAddProcess
from report.commons.connect_kudu2 import prod_execute_sql
from report.commons.settings import CONN_TYPE

log = get_logger(__name__)


class Scheduler(threading.Thread):
    def __init__(self, name, delay):
        threading.Thread.__init__(self)
        self.name = name
        self.delay = delay

    def run(self):
        # print("开始线程：" + self.name)
        while True:
            # self.show_time(self.name)
            self.check_execute_step05()

            time.sleep(60 * 5)

        print("退出线程：" + self.name)

    def check_execute_step05(self):
        """
        判断流程表中已经执行了第5步
        :return:
        """

        try:
            columns_ls = ['process_id', 'process_status', 'daily_start_date', 'daily_end_date', 'step_number',
                          'operate_desc', 'orgin_source', 'destin_source', 'importdate']
            columns_str = ",".join(columns_ls)

            t = get_current_time()
            data = t.split(' ')
            year_month_day = str(data[0]).replace('-', '')

            # year_month_day = '20220118'

            sel_sql = f"""
            select {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE ( from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{year_month_day}' OR importdate = '{year_month_day}' ) AND 
            process_status = 'sucess'  ORDER BY step_number ASC
            """
            log.info(sel_sql)
            records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)

            is_execute_step05 = False
            is_excute_step6789 = False
            if len(records) > 0:
                for record in records:
                    # print(record)

                    step_number = str(record[4])
                    if step_number == '5':
                        is_execute_step05 = True

                    if step_number in ['6', '7', '8', '9']:
                        is_excute_step6789 = True

            else:
                log.info('*** 没有查询数据 ***')

            # 执行了前5步，但是没有执行过 6,7,8,9步，就开始执行任务
            if is_execute_step05 and not is_excute_step6789:
                self.task()
            else:
                log.info('*** 不执行第6,7,8,9 步的任务 ***')

        except Exception as e:
            print(e)

    def show_time(self, text='task'):
        # t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        t = get_current_time()
        data = t.split(' ')
        hour_minute = data[1].split(':')
        hour = hour_minute[0]
        minute = hour_minute[1]

        # print('hour_minute=> ', hour_minute)
        # print('hour=> ', hour)
        # print('minute=> ', minute)

        log.info('*' * 30)
        log.info('*** {} ---> {}'.format(text, t))
        log.info('*' * 30)
        print()

    def task(self):
        log.info("*" * 30)
        log.info('***** 开始执行第6步，增量数据流程 *****')
        log.info("*" * 30)
        increment_process = IncrementAddProcess()
        increment_process.exec_steps()


def exec_scheduler():
    """
    定时调度
    :return:
    """
    schedule = Scheduler('thr', 10)
    schedule.start()


if __name__ == '__main__':
    # show_time()

    exec_scheduler()
