安装 Flask-migrate
> pip install flask-migrate

app.py
```
#encoding:utf-8
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI']='mysql+pymysql://root:root@127.0.0.1:3306/db_6_14'
#'mysql+pymysql://root:root@127.0.0.1:3306/'
# 如果设置成 True (默认情况)，Flask-SQLAlchemy 将会追踪对象的修改并且发送信号。这需要额外的内存， 如果不必要的可以禁用它。
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
db=SQLAlchemy(app)

@app.route('/')
def hello_world():
    return 'Hello World!'
if __name__ == '__main__':
    app.run()
```

model.py
```
#encoding:utf8
from flask_sqlalchemy import SQLAlchemy
from app import db

class User(db.Model):
    user_id = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String(60), nullable=False)
    user_password = db.Column(db.String(30), nullable=False)

```

manager.py
```
#encoding:utf8
from flask_script import Manager #flask 脚本
from flask_migrate import Migrate,MigrateCommand #flask 迁移数据
from app import app,db
from model import User

migrate = Migrate(app,db)#传入2个对象一个是flask的app对象，一个是SQLAlchemy
manager = Manager(app)
manager.add_command('db',MigrateCommand)#给manager添加一个db命令并且传入一个MigrateCommand的类

if __name__=='__main__':
    manager.run()




```



初始化迁移脚本环境
> python manager.py db init

对数据库中表进行更新
> python manager.py db migrate

> python manager.py db upgrade