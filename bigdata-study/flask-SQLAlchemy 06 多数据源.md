http://www.pythondoc.com/flask-sqlalchemy/binds.html

```
class TableName(db.Model):
     __tablename__ = 'tablename'
    __bind_key__ = 'db1' #在此处执行即可
```

### 使用默认的数据源

```
db.drop_all(bind=None)
db.create_all(bind=None)
```

### 解决多数据库存在同表表名的时候存在异常信息提示

解决：添加： table_args = {"useexisting": True}

```
class Book(Base):
    __bind_key__ = 'lincms4'
    __tablename__ = 'book'  # 未设置__bind_key__,则采用默认的数据库引擎
    __table_args__ = {"useexisting": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(50), nullable=False)
    author = Column(String(30), default='未名')
    summary = Column(String(1000))
    image = Column(String(50))
    imagessss = Column(String(50))
```

### 查询单个

```
Group_Check.query.fliter(Group_Check.id == id).first()
```

### 查询所有
```
Group_Check.query.fliter(Group_Check.id == id).all()
```

### 条件查询
```
Group_Check.query.fliter(db.and_(Group_Check.id == id, Group_Check.state==1))
```

