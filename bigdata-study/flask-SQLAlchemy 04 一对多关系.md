### 例子1
一个作者可以编著多本图书的一对多关系

```
# -*- coding: utf-8 -*-

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import config
app = Flask(__name__)
app.config.from_object(config)
db=SQLAlchemy(app)
#定义用户表
#定义模型类-作者类 ---主表
class Writer(db.Model):
    __tablename__='writer1'
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String(50),nullable=False)
     #设置relationship属性方法，建立模型关系，第一个参数为多方模型的类名，添加backref可以实现多对一的反向查询
    books = db.relationship('Book',backref='writers')

#定义模型类-图书类 --- 从表
class Book(db.Model):
    __tablename__='books1'
    id = db.Column(db.Integer,primary_key=True)
    title = db.Column(db.String(50),nullable=False)
    publishing_office = db.Column(db.String(100), nullable=False)
    isbn = db.Column(db.String(50), nullable=False)
    #设置外键指向一方的主键，建立关联关系
    writer_id= db.Column(db.Integer, db.ForeignKey('writer1.id'))

# http://127.0.0.1:5000/add
@app.route('/add')
def add():
    db.drop_all()
    db.create_all()
    #添加两条作者数据
    user1 = Writer(name="李兴华")
    user2 = Writer(name="Sweigart")
    db.session.add(user1)
    db.session.add(user2)
    #添加两条图书信息
    book1=Book(title='名师讲坛——Java开发实战经典（第2版）', publishing_office='清华大学出版社', isbn='9787302483663',writer_id='1')
    book2 = Book(title='android开发实战', publishing_office='清华大学出版社', isbn='9787302281559',writer_id='1')
    book3 =Book(title='Python游戏编程快速上手', publishing_office='人民邮电大学出版社',isbn='9787115466419',writer_id='2')
    db.session.add(book1)
    db.session.add(book2)
    db.session.add(book3)
    db.session.commit()
    return '添加数据成功！'

# http://127.0.0.1:5000/select
@app.route('/select')
def select():
    writer = Writer.query.filter(Writer.id == '1').first()
    book = writer.books
    for k in book:
        print(k)
        print(k.title)

    print('-----------------------')
    book=Book.query.filter(Book.id=='1').first()
    writer=book.writers
    print(writer.name)
    return "查询数据成功！"

# http://127.0.0.1:5000/delete
@app.route('/delete')
def delete():
    # 删除报错，删除从表后才能删除主表中的数据
    #writer = Writer.query.filter(Writer.id=='1').first()
    #db.session.commit(writer)

    book = Book.query.filter(Book.id == '2').first()
    db.session.commit()

    return '删除数据成功'

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.config['SQLALCHEMY_ECHO'] = True
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.run()

```

### 例子2
```
# -*- coding: utf-8 -*-

# -*- coding: utf-8 -*-
from flask import Flask,flash,url_for,redirect
from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
import config

app = Flask(__name__)
app.config.from_object(config)
db=SQLAlchemy(app)

class Authors(db.Model):
   __tablename__ = "author"
   id = db.Column(db.Integer,primary_key=True)
   name = db.Column(db.String(40),unique=True)
   # 利用关系属性来连接两个表
   books = db.relationship("Books")

class Books(db.Model):
    __tablename__ = "book"
    id = db.Column(db.Integer,primary_key=True)
    name = db.Column(db.String(40),unique=True)
    aut_id = db.Column(db.Integer,db.ForeignKey("author.id"))

# http://127.0.0.1:5000/add
@app.route('/add')
def add():
    init_table()

    stu1 = Authors(name="张三")
    stu2 = Authors(name="李四")
    stu3 = Authors(name="王二")
    cou1 = Books(name="《葵花宝典》")
    cou2 = Books(name="《九阴真经》")
    cou3 = Books(name="《凌波微步》")
    cou4 = Books(name="《一阳指》")
    cou5 = Books(name="《乾坤大挪移》")
    cou6 = Books(name="《狮吼功》")
    cou7 = Books(name="《金鸡独立》")
    cou8 = Books(name="《九阴白骨爪》")
    cou9 = Books(name="《蛤蟆功》")

    stu1.books.append(cou1)
    stu1.books.append(cou2)
    stu2.books.append(cou3)
    stu2.books.append(cou4)
    stu2.books.append(cou5)
    stu2.books.append(cou6)
    stu3.books.append(cou7)
    stu3.books.append(cou8)
    stu3.books.append(cou9)

    db.session.add_all([stu1, stu2, stu3, cou1, cou2, cou3, cou4, cou5, cou6, cou7, cou8, cou9])
    db.session.commit()
    return '添加数据成功！'

# http://127.0.0.1:5000/select
@app.route('/select')
def select():

    return "查询数据成功！"

# http://127.0.0.1:5000/delete
@app.route('/delete')
def delete():
    print('--- delete ---')
    # 获取需要删除的课程ｉｄ
    book_id = 1
    try:
        book_id = int(book_id)
    except Exception as e:
        flash("参数错误")
        return redirect(url_for("index"))
    try:
        # 查询获得数据的对象
        book = Books.query.filter_by(id=book_id).first()
        print('*** book=> ',book)
        author = Authors.query.filter_by(id=book.aut_id).first()
        author.books.remove(book)
        db.session.commit()
    except Exception as e:
        flash("操作错误")
        return redirect(url_for("index"))

    return '删除数据成功'

# http://127.0.0.1:5000/delete2
@app.route('/delete2')
def delete2():
    #db.session.query(Books).filter(Books.id == 1).delete()
    db.session.query(Authors).filter(Authors.id == 1).delete()
    db.session.commit()
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