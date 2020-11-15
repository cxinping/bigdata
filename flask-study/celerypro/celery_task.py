# -*- coding: utf-8 -*-

import celery
import time

backend = 'redis://127.0.0.1:6379/1'
broker = 'redis://127.0.0.1:6379/2'

cel = celery.Celery('test',backend=backend,broker=broker)

def get_current_time():
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')

@cel.task
def send_email(name):
    print("{} 向{}发送邮件...".format(get_current_time(name)) )
    time.sleep(5)
    print("{} 向{}发送邮件完成".format(get_current_time(name)))
    return "send mail ok"

