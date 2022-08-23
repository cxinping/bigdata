
访问地址
```
http://0.0.0.0:8000/
```

创建表结构，迁移数据
```
python3 manage.py migrate   # 创建表结构

python3 manage.py makemigrations app01  # 让 Django 知道我们在我们的模型有一些变更

python3 manage.py migrate app01   # 创建表结构
```