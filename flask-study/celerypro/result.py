# -*- coding: utf-8 -*-

from celery.result import AsyncResult
from celery_task import cel

async_result=AsyncResult(id="c6ddd5b7-a662-4f0e-93d4-ab69ec2aea5d", app=cel)

if async_result.successful():
    result = async_result.get()
    print(result)
    # result.forget() # 将结果删除
elif async_result.failed():
    print('执行失败')
elif async_result.status == 'PENDING':
    print('任务等待中被执行')
elif async_result.status == 'RETRY':
    print('任务异常后正在重试')
elif async_result.status == 'STARTED':
    print('任务已经开始被执行')