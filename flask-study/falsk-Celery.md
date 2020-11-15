
##  apscheduler

> https://www.cnblogs.com/yueerwanwan0204/p/5480870.html

## 在 Flask 中使用 Celery

> http://www.pythondoc.com/flask-celery/first.html

> https://www.cnblogs.com/pyedu/p/12461819.html

> https://flask.palletsprojects.com/en/1.0.x/patterns/celery/

celery -A celery_tasks worker  -l info -P eventlet


安装 Celery

```
pip install celery
pip install eventlet
pip install -i https://pypi.tuna.tsinghua.edu.cn/simple eventlet

安装redis
pip install redis

启动redis服务
    redis-server
```



