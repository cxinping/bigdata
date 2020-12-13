# -*- coding: utf-8 -*-

from datetime import datetime

from .exts import db

class Book(db.Model):
    __tablename__='book'
    id=db.Column(db.Integer,primary_key=True,autoincrement=True)
    title=db.Column(db.String(50),nullable=False)
    publishing_office=db.Column(db.String(100),nullable=False)
    price=db.Column(db.String(30),nullable=False)
    isbn=db.Column(db.String(50),nullable=False)
    storage_time = db.Column(db.DateTime, default=datetime.now)  # 入库时间