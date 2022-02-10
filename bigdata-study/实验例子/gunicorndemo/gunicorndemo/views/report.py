# -*- coding: utf-8 -*-

"""
Created on

@author:
"""

from flask import Blueprint, jsonify
from flask import render_template
from gunicorndemo.commons.util import get_current_time
from gunicorndemo.models import Book
from gunicorndemo.exts import db

report_bp = Blueprint('report', __name__)

# http://127.0.0.1:8888/report/hello
@report_bp.route('/hello')
@report_bp.route('/hello/<name>')
def hello(name=None):
    return render_template('index.html', name=name)

# http://127.0.0.1:8888/report/hello2
# http://192.168.11.10:8888/report/hello2
@report_bp.route("/hello2",methods=['GET', 'POST'])
def hello2():
    return "<h1 style='color:blue;text-align: center;'>Hello world! {time}</h1>".format(time=(get_current_time()))

# http://127.0.0.1:8888/report/line
@report_bp.route('/line')
def show_line():
    return render_template('echarts_line.html')

# http://127.0.0.1:8888/report/pie
@report_bp.route('/pie')
def show_pie():
    return render_template('echarts_pie.html')

# http://127.0.0.1:8888/report/init/db
@report_bp.route('/init/db', methods=['GET'])
def init_db():
    db.drop_all(bind=None)
    db.create_all(bind=None)

    return "init db "

# http://127.0.0.1:8888/report/add
@report_bp.route('/add')
def add():
    book1=Book(title='Python基础教程（第3版）',publishing_office='人民邮电出版社',price='68.30',isbn='9787115474889')
    book2 = Book(title='Python游戏编程快速上手 第4版', publishing_office='人民邮电出版社', price='54.50', isbn='9787115466419')
    book3 = Book(title='JSP+Servlet+Tomcat应用开发从零开始学', publishing_office='清华大学出版社', price='68.30',isbn='9787302384496')
    db.session.add(book1)
    db.session.add(book2)
    db.session.add(book3)
    db.session.commit()
    return '添加数据成功！'

# http://127.0.0.1:8888/report/select
@report_bp.route('/select')
def select():
    # result=Book.query.filter(Book.id=='1').first()
    result = Book.query.filter(Book.publishing_office == '人民邮电出版社').all()
    for books in result:
        print(books.title,books.publishing_office)
    return "查询数据成功！"

# http://127.0.0.1:8888/report/edit
@report_bp.route('/edit')
def edit():
    book1=Book.query.filter(Book.id=='2').first()#查询出id=2的记录
    book1.price=168#修改价格
    db.session.commit()
    return "修改数据成功！"

# http://127.0.0.1:8888/report/delete
@report_bp.route('/delete')
def delete():
    book1=Book.query.filter(Book.id=='1').first()
    db.session.delete(book1)
    db.session.commit()
    return '删除数据成功'