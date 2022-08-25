# -*- coding: utf-8 -*-
from main import app
import time


import logging
log = logging.getLogger("django")

@app.task  # name表示设置任务的名称，如果不填写，则默认使用函数名做为任务名
def send_sms(mobile):
    """发送短信"""
    print("向手机号%s发送短信成功!"%mobile)
    time.sleep(5)

    return "send_sms OK"

@app.task  # name表示设置任务的名称，如果不填写，则默认使用函数名做为任务名
def send_sms2(mobile):
    print("向手机号%s发送短信成功!" % mobile)
    time.sleep(5)

    return "send_sms2 OK"

# celery -A main worker --loglevel=info
# celery worker -A study_django.main -l info -P eventlet

# celery -A study_django.main worker --loglevel=info

# 启动Celery的命令
# 强烈建议切换目录到mycelery根目录下启动
# celery -A mycelery.main worker --loglevel=info
