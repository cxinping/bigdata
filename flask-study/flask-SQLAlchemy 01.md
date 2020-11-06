###安装 flask-sqlalchemy

> pip install flask-sqlalchemy

> pip install -i https://pypi.tuna.tsinghua.edu.cn/simple flask-sqlalchemy

在Flask Web应用程序中使用原始SQL对数据库执行CRUD操作可能很乏味。相反，Python工具包 SQLAlchemy 是一个功能强大的 OR映射器 ，为应用程序开发人员提供了SQL的全部功能和灵活性。Flask- SQLAlchemy是Flask扩展，它将对SQLAlchemy的支持添加到Flask应用程序中。

### 什么是ORM（对象关系映射）？

大多数编程语言平台是面向对象的。另一方面，RDBMS服务器中的数据以表格形式存储。对象关系映射是一种将对象参数映射到底层RDBMS表结构的技术。ORM API提供了执行CRUD操作的方法，而无需编写原始SQL语句。

### 数据库驱动字符串

config.py
```
#encoding:utf-8
HOST = '127.0.0.1'
PORT = '3306'
USERNAME = 'root'
PASSWORD = 'root'
DATABASE = 'library6_11_02'
DB_URI = 'mysql+pymysql://{username}:{password}@{host}:{port}/{db}?charset-utf8'.format(username=USERNAME, password=PASSWORD, host=HOST,  port=PORT,                      db=DATABASE)                                                            
```

```
from flask import Flask
from flask_sqlalchemy import SQLAlchemy

from datetime import datetime

app = Flask(__name__)

app.config['SQLALCHEMY_DATABASE_URI'] =  'postgresql://postgres:123456@127.0.0.1/testdb1'
# 动态追踪修改设置，如未设置只会提示警告
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
#查询时会显示原始SQL语句
app.config['SQLALCHEMY_ECHO'] = True

db = SQLAlchemy(app)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True)
    email = db.Column(db.String(120), unique=True)
    gender = db.Column(db.String(1), unique=False)

    def __init__(self, username, email,gender):
        self.username = username
        self.email = email
        self.gender = gender

    def __repr__(self):
        return '<User %r>' % self.username

class Post(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String(80))
    body = db.Column(db.Text)
    pub_date = db.Column(db.DateTime)

    category_id = db.Column(db.Integer, db.ForeignKey('category.id'))
    category = db.relationship('Category',
        backref=db.backref('posts', lazy='dynamic'))

    def __init__(self, title, body, category, pub_date=None):
        self.title = title
        self.body = body
        if pub_date is None:
            pub_date = datetime.utcnow()
        self.pub_date = pub_date
        self.category = category

    def __repr__(self):
        return '<Post %r>' % self.title

class Category(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50))

    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return '<Category %r>' % self.name

if __name__ == '__main__':
    # 创建表和数据库
    db.drop_all()
    db.create_all()

    admin = User('admin', 'admin@example.com' , '男')
    guest = User('guest', 'guest@example.com' , '女')
    #db.session.add(admin)
    #db.session.add(guest)
    #db.session.commit()

    py = Category('Python')
    p = Post('Hello Python!', 'Python is pretty cool', py)
    db.session.add(py)
    db.session.add(p)
    #db.session.commit()

```