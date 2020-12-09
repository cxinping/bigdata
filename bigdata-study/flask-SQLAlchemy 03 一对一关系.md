### 用户和借书证构成了一对一的关系


config.py
```
DB_URI = 'postgresql://postgres:123456@127.0.0.1/testdb2'
SQLALCHEMY_DATABASE_URI = DB_URI
# 动态追踪修改设置，如未设置只会提示警告
SQLALCHEMY_TRACK_MODIFICATIONS=False
#查询时会显示原始SQL语句
SQLALCHEMY_ECHO = True


```

app2.py
```
# -*- coding: utf-8 -*-
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import config

app = Flask(__name__)
app.config.from_object(config)
db=SQLAlchemy(app)

#定义用户表 --- 主表
class User(db.Model):
    __tablename__ = 'user1'
    id=db.Column(db.Integer,primary_key=True,autoincrement=True)
    username=db.Column(db.String(50),nullable=False)#用户名
    password=db.Column(db.String(50),nullable=False)#密码
    phone=db.Column(db.String(11),nullable=False)#电话
    email=db.Column(db.String(30),nullable=False)#邮箱
    reg_time=db.Column(db.DateTime,default=datetime.now)#注册时间

#定义借书证表 -- 从表
class  Lib_card(db.Model):
    __tablename__='lib_card1'
    id = db.Column(db.Integer, primary_key=True, autoincrement=True)
    card_id=db.Column(db.Integer,nullable=False)#借书证id
    papers_type=db.Column(db.String(50),nullable=False)#何种证件办理
    borrow_reg_time=db.Column(db.DateTime,default=datetime.now)#证件办理时间
    user_id = db.Column(db.Integer, db.ForeignKey('user1.id'))
    users = db.relationship('User', backref= db.backref('cards'), uselist=False)

# http://127.0.0.1:5000/add
@app.route('/add')
def add():
    init_table()

    #添加两条用户数据
    user1 = User(username="张三", password="111111", phone="13888888888", email="10086@qq.com")
    user2 = User(username="李四", password="123456", phone="13777777777", email="10000@qq.com")
    db.session.add(user1)
    db.session.add(user2)

    #添加三条借书证信息
    card1 = Lib_card(card_id='18001', user_id='1', papers_type='身份证')
    card2 =Lib_card(card_id='18002', user_id='2', papers_type='身份证')

    db.session.add(card1)
    db.session.add(card2)
    db.session.commit()
    return '添加数据成功！'

# http://127.0.0.1:5000/select
@app.route('/select')
def select():
    user = User.query.filter(User.username == '张三').first()
    art = user.cards
    for k in art:
        #print(k)
        print(k.card_id)

    print('----------------')

    card = Lib_card.query.filter(Lib_card.card_id=='18001').first()
    user = card.users
    print(user.username)
    return "查询数据成功！"

# http://127.0.0.1:5000/delete
@app.route('/delete')
def delete():

    #lib_card = Lib_card.query.filter(Lib_card.id=='1').first()
    #db.session.delete(lib_card)

    user = User.query.filter(User.id == '2').first()
    db.session.delete(user)
    db.session.commit()

    return '删除数据成功'

@app.route('/')
def hello_world():
    return 'Hello World!'

def init_table():
    db.drop_all()
    db.create_all()
    print('--- ok ---')

if __name__ == '__main__':
    app.config['SQLALCHEMY_ECHO'] = True
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.run(debug=True)
    # 查询时会显示原始SQL语句





```