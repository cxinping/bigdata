图书和图书标签（上架建议标签）的多对多关系

```
# -*- coding: utf-8 -*-

from flask import Flask
from flask_sqlalchemy import SQLAlchemy
import config

app = Flask(__name__)
app.config.from_object(config)
db=SQLAlchemy(app)

book_tag = db.Table('book_tag',
    db.Column('book_id',db.Integer,db.ForeignKey('book.id'),primary_key=True),
    db.Column('tag_id',db.Integer,db.ForeignKey('shelfing.id'),primary_key=True)
        )

#定义模型类-图书类
class Book(db.Model):
    __tablename__='book'
    id=db.Column(db.Integer,primary_key=True,autoincrement=True)
    name = db.Column(db.String(50), nullable=False)
    #设置relationship属性方法，建立模型关系，第一个参数为多方模型的类名，secondary代表中间表
    #第三个参数表示反向引用
    tags = db.relationship('Shelfing',secondary= book_tag,backref = db.backref('books'))

#定义模型类-图书上架建议（标签）类
class Shelfing(db.Model):
    __tablename__='shelfing'
    id=db.Column(db.Integer,primary_key=True,nullable=False)
    tag=db.Column(db.String(50),nullable=False)

#  http://127.0.0.1:5000/add
@app.route('/add')
def add():
    db.drop_all()
    db.create_all()

    book1=Book(name='Java开发')
    book2=Book(name='Python游戏编程快速上手')
    book3=Book(name='文艺范')
    tag1=Shelfing(tag='文艺')
    tag2 = Shelfing(tag='计算机')
    tag3=Shelfing(tag='技术')
    book1.tags.append(tag2)
    book1.tags.append(tag3)
    book2.tags.append(tag3)
    book3.tags.append(tag1)
    db.session.add_all([book1,book2,book3,tag1,tag2,tag3])
    db.session.commit()
    return '数据添加成功！'

@app.route('/select')
def select():
    #查询Java开发这本书的上架标签
    book = Book.query.filter(Book.name == 'Java开发').first()
    tag = book.tags
    for k in tag:
        print(k.tag)
    #查询标签=技术的所有图书
    tag=Shelfing.query.filter(Shelfing.tag=='技术').first()
    book=tag.books
    for k in book:
        print(k.name)
    return '查询成功！'

@app.route('/')
def hello_world():
    return 'Hello World!'

if __name__ == '__main__':
    app.config['SQLALCHEMY_ECHO'] = True
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True
    app.run()

```