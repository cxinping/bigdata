
#  apscheduler

> https://www.cnblogs.com/yueerwanwan0204/p/5480870.html

# 在 Flask 中使用 Celery

> http://www.pythondoc.com/flask-celery/first.html

> https://www.cnblogs.com/pyedu/p/12461819.html

> https://flask.palletsprojects.com/en/1.0.x/patterns/celery/




## 安装 Celery

```
pip install celery
pip install eventlet
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple eventlet

安装redis
pip install redis

启动redis服务
redis-server

```

## Celery
Celery是一个简单、灵活且可靠的，处理大量消息的分布式系统，专注于实时处理的异步任务队列，同时也支持任务调度。

Celery的架构由三部分组成，消息中间件（message broker），任务执行单元（worker）和任务执行结果存储（task result store）组成。

消息中间件

Celery本身不提供消息服务，但是可以方便的和第三方提供的消息中间件集成。包括，RabbitMQ, Redis等等

任务执行单元

Worker是Celery提供的任务执行的单元，worker并发的运行在分布式的系统节点中。

任务结果存储

Task result store用来存储Worker执行的任务的结果，Celery支持以不同方式存储任务的结果，包括AMQP, redis等

## 使用场景
celery是一个强大的 分布式任务队列的异步处理框架，它可以让任务的执行完全脱离主程序，甚至可以被分配到其他主机上运行。我们通常使用它来实现异步任务（async task）和定时任务（crontab)。

异步任务：将耗时操作任务提交给Celery去异步执行，比如发送短信/邮件、消息推送、音视频处理等等

定时任务：定时执行某件事情，比如每天数据统计

## Celery执行异步任务

创建异步任务执行文件celery_task:

```

import celery
import time

backend='redis://127.0.0.1:6379/1'
broker='redis://127.0.0.1:6379/2'
cel=celery.Celery('test',backend=backend,broker=broker)
@cel.task
def send_email(name):
    print("向%s发送邮件..."%name)
    time.sleep(5)
    print("向%s发送邮件完成"%name)
    return "ok"　
	
	

```

> celery -A celery_tasks worker  -l info -P eventlet







## 多任务结构








