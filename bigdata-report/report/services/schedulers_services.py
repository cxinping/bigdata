# -*- coding: utf-8 -*-
import time
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from report.commons.logging import get_logger

log = get_logger(__name__)


def exec_scheduler():
    """
    定时调度
    :return:
    """
    scheduler = BackgroundScheduler()
    # scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.add_job(show_time, 'interval', minutes=1, start_date='2022-01-07 08:51:00',
                      end_date='2200-03-29 14:00:10')
    scheduler.start()

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(10)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()


def show_time(text='task'):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    log.info('{} ---> {}'.format(text, t))


def exec_task():
    pass


if __name__ == '__main__':
    show_time()