# -*- coding: utf-8 -*-
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from report.commons.logging import get_logger
import threading
import time

log = get_logger(__name__)


class Scheduler(threading.Thread):
    def __init__(self, name, delay):
        threading.Thread.__init__(self)
        self.name = name
        self.delay = delay

    def run(self):
        print("开始线程：" + self.name)
        while True:
            self.show_time(self.name )
            time.sleep(5)

        print("退出线程：" + self.name)

    def show_time(self, text='task'):
        t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
        log.info('*' * 30)
        log.info('*** {} ---> {}'.format(text, t))
        log.info('*' * 30)

    def task(self):
        pass


def exec_scheduler():
    """
    定时调度
    :return:
    """
    # show_time()

    # scheduler = BackgroundScheduler()
    # scheduler.add_job(show_time, 'interval', seconds=60)
    # # scheduler.add_job(show_time, 'interval', minutes=1, start_date='2022-01-12 01:35:26',end_date='2200-03-29 14:00:10')
    # scheduler.start()
    #
    # try:
    #     # This is here to simulate application activity (which keeps the main thread alive).
    #     while True:
    #         time.sleep(60)
    #         t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    #         print(t)
    # except (KeyboardInterrupt, SystemExit):
    #     # Not strictly necessary if daemonic mode is enabled but should be done if possible
    #     scheduler.shutdown()
    schedule = Scheduler('thr', 10)
    schedule.start()


def show_time(text='task'):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log.info('*' * 30)
    log.info('*** {} ---> {}'.format(text, t))
    log.info('*' * 30)


if __name__ == '__main__':
    #show_time()

    exec_scheduler()
