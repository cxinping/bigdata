# CRUD
config.py

```
DB_URI = 'postgresql://postgres:123456@127.0.0.1/testdb2'
SQLALCHEMY_DATABASE_URI = DB_URI
# 动态追踪修改设置，如未设置只会提示警告
SQLALCHEMY_TRACK_MODIFICATIONS=False
#查询时会显示原始SQL语句
SQLALCHEMY_ECHO = True
```

db_demo1.py
```
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import config
app = Flask(__name__)
app.config.from_object(config)
db=SQLAlchemy(app)

class Book(db.Model):
    __tablename__='book'
    id=db.Column(db.Integer,primary_key=True,autoincrement=True)
    title=db.Column(db.String(50),nullable=False)
    publishing_office=db.Column(db.String(100),nullable=False)
    price=db.Column(db.String(30),nullable=False)
    isbn=db.Column(db.String(50),nullable=False)
    storage_time = db.Column(db.DateTime, default=datetime.now)  # 入库时间
db.create_all()
@app.route('/add')
def add():
    book1=Book(title='Python基础教程（第3版）',publishing_office='人民邮电出版社',price='68.30',isbn='9787115474889')
    book2 = Book(title='Python游戏编程快速上手 第4版', publishing_office='人民邮电出版社', price='54.50', isbn='9787115466419')
    book3 = Book(title='JSP+Servlet+Tomcat应用开发从零开始学', publishing_office='清华大学出版社', price='68.30',isbn='9787302384496')
    db.session.add(book1)
    db.session.add(book2)
    db.session.add(book3)
    db.session.commit()
    return '添加数据成功！'
@app.route('/select')
def select():
    # result=Book.query.filter(Book.id=='1').first()
    result = Book.query.filter(Book.publishing_office == '人民邮电出版社').all()
    for books in result:
        print(books.title,books.publishing_office)
    return "查询数据成功！"
@app.route('/edit')
def edit():
    book1=Book.query.filter(Book.id=='2').first()#查询出id=2的记录
    book1.price=168#修改价格
    db.session.commit()
    return "修改数据成功！"
@app.route('/delete')
def delete():
    book1=Book.query.filter(Book.id=='9').first()
    db.session.delete(book1)
    db.session.commit()
    return '删除数据成功'

@app.route('/')
def hello_world():
    return 'Hello World!'
if __name__ == '__main__':
    app.run(debug=True)


```

# 自定义表的id为UUID

```
from werkzeug.security import generate_password_hash,check_password_hash
from datetime import datetime

from app import db #导入db 对象
import uuid

def generate_uuid():
       uuid_str = str(uuid.uuid4()).replace('-', '')
       return uuid_str

class User(db.Model):#定义User类
       __tablename__='bmo_user'#表的别名

       id = db.Column(db.String(50),primary_key=True, default=generate_uuid )#定义id字段
       username = db.Column(db.String(50),nullable=True)#定义username字段
       password = db.Column(db.String(100),nullable=True)#定义password字段
       telephone = db.Column(db.String(11), nullable=True)#定义telephone字段
       gender = db.Column(db.String(10), nullable=True)
       reg_time = db.Column(db.DateTime,default=datetime.now)
       login_time = db.Column(db.DateTime,default=datetime.now)
```