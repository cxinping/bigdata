# -*- coding: utf-8 -*-
import time
#from apscheduler.schedulers.blocking import BlockingScheduler
#from apscheduler.schedulers.background import BackgroundScheduler
from report.commons.logging import get_logger
import threading
import time
from report.commons.tools import get_current_time,get_current_year_month_day
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
        #print("开始线程：" + self.name)
        while True:
            #self.show_time(self.name)
            self.check_current_finance_data_process()

            time.sleep(60 * 5)

        print("退出线程：" + self.name)

    def check_current_finance_data_process(self):
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
            year_month_day = str(data[0]).replace('-','')
            #print(year_month_day)

            sel_sql = f"select {columns_str} FROM 01_datamart_layer_007_h_cw_df.finance_data_process WHERE from_unixtime(unix_timestamp(to_date(importdate),'yyyy-MM-dd'),'yyyyMMdd') = '{year_month_day}' AND process_status = 'sucess'  ORDER BY step_number ASC  "

            records = prod_execute_sql(conn_type=CONN_TYPE, sqltype='select', sql=sel_sql)
            for record in records:
                print(record)
                print()
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


def show_time(text='task'):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log.info('*' * 30)
    log.info('*** {} ---> {}'.format(text, t))
    log.info('*' * 30)


if __name__ == '__main__':
    # show_time()

    exec_scheduler()

