# -*- coding: utf-8 -*-

from apscheduler.schedulers.blocking import BlockingScheduler
from datetime import datetime
import time
import os
from apscheduler.schedulers.background import BackgroundScheduler

def job(text='job'):
    t = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print('{} ---> {}'.format(text, t))


def tick():
    print('****** Tick! The time is: %s' % datetime.now())


if __name__ == '__main__':
    job()

    scheduler = BackgroundScheduler()
    #scheduler.add_job(tick, 'interval', seconds=3)
    scheduler.add_job(tick, 'interval', minutes=1, start_date='2022-01-07 08:55:00', end_date='2200-03-29 14:00:10')
    scheduler.start()

    # print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))
    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(5)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()
