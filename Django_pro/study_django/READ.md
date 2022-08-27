
访问地址
```
http://0.0.0.0:8000/
```

创建表结构，迁移数据
```
python3 manage.py migrate   # 创建表结构

python3 manage.py makemigrations app01  # 让 Django 知道我们在我们的模型有一些变更

python3 manage.py migrate app01   # 创建表结构

python manage.py makemigrations 
python manage.py migrate
```


测试url
```
http://127.0.0.1:8000/testdb

```

在django项目下启动Celery的worker
```
celery -A mycelery.main worker -l info -P eventlet

celery -A mycelery.main worker --loglevel=info -P eventlet

```

python3 manage.py makemigrations 
python3 manage.py migrate