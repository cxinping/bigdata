# -*- coding: utf-8 -*-
import os
from celery import Celery
# 创建celery实例对象
app = Celery("sms")

# 把celery和django进行组合，识别和加载django的配置文件
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'study_django.settings.dev')

# 通过app对象加载配置
app.config_from_object("study_django.config")

# 加载任务
# 参数必须必须是一个列表，里面的每一个任务都是任务的路径名称
# app.autodiscover_tasks(["任务1","任务2"])
app.autodiscover_tasks(["study_django.sms",])

# 加载任务
# 参数必须必须是一个列表，里面的每一个任务都是任务的路径名称
# app.autodiscover_tasks(["任务1","任务2"])
app.autodiscover_tasks(["study_django.sms",])