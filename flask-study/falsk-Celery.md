
#  apscheduler

> https://www.cnblogs.com/yueerwanwan0204/p/5480870.html

# 在 Flask 中使用 Celery

> https://www.celerycn.io/ru-men/celery-chu-ci-shi-yong

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

创建异步任务执行文件 celery_task.py

```
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
    print("{} 向{}发送邮件...".format(get_current_time() , name) )
    time.sleep(5)
    print("{} 向{}发送邮件完成".format(get_current_time(),name))
    return "send mail ok"

@cel.task
def send_msg(name):
    print("{} 向{}发送短信...".format(get_current_time() , name) )
    time.sleep(5)
    print("{} 向{}发送短信完成".format(get_current_time(),name))
    return "send sms ok"

```

执行任务文件 produce_task.py

```
from celery_task import send_email , send_msg

result = send_email.delay("wang")
print('result.id=',result.id)

result2 = send_msg.delay("li")
print('result2.id=', result2.id)

```

执行 produce_task.py 获得以下结果

```
result.id= a0670116-2936-474a-b0db-56b6a58bc1ac
result2.id= 94df5dab-b47f-47c6-9214-f1a8cbb90bdb
```

异步任务文件命令执行

> celery -A celery_task worker  -l info


创建 check_result.py，查看任务执行结果

```
from celery.result import AsyncResult
from celery_task import cel

async_result=AsyncResult(id="a0670116-2936-474a-b0db-56b6a58bc1ac", app=cel)

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
```

执行 check_result.py 获得以下结果

```
send mail ok
```

## 多任务结构

创建目录 celery_tasks，在 celery_tasks 目录下新建 celery.py
```


```




> celery -A celery_tasks worker  -l info -P eventlet



# 参考资料

https://www.cnblogs.com/miss103/p/13275550.html

https://blog.csdn.net/lixingdefengzi/article/details/51769731

https://www.pianshen.com/article/2176289575/

https://blog.csdn.net/weixin_43162402/article/details/83314877

https://www.jianshu.com/p/9e422d9f1ce2

https://www.debug8.com/python/t_14161.html

https://www.crifan.com/celery_task_integrated_to_flask_app/

https://zhuanlan.zhihu.com/p/28829462

https://zhuanlan.zhihu.com/p/22304455

https://www.cnblogs.com/pyedu/p/12461819.html

https://flask.palletsprojects.com/en/1.0.x/patterns/celery/

https://www.jianshu.com/p/bdd9dcbf1e21

http://www.pythondoc.com/flask-celery/first.html

https://www.cnblogs.com/pyedu/p/12461819.html




