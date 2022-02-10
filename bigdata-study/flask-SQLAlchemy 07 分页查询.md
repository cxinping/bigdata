# SQLAlchemy的分页操作

sqlalchemy中使用query查询，而flask-sqlalchemy中使用basequery查询，他们是子类与父类的关系

假设 page_index=1,page_size=10；所有分页查询不可以再跟first(),all()等

## 1.用offset()设置索引偏移量,limit()限制取出量

```
db.session.query(User.name).filter(User.email.like('%'+email+'%')).limit(page_size).offset((page_index-1)*page_size)

#filter语句后面可以跟order_by语句
```

## 2.用slice(偏移量，取出量)函数

```
db.session.query(User.name).filter(User.email.like('%'+email+'%')).slice((page_index - 1) * page_size, page_index * page_size)
#filter语句后面可以跟order_by语句
```

## 3.用paginate(偏移量，取出量)函数,用于BaseQuery

```
user_obj=User.query.filter(User.email.like('%'+email+'%')).paginate(int(page_index), int(page_size),False)
#遍历时要加上items 
object_list =user_obj.items
```

## 4.filter中使用limit

```
db.session.query(User.name).filter(User.email.like('%'+email+'%') and limit (page_index - 1) * page_size, page_size)
#此处不能再跟order_by语句，否则报错

```

## 删除行 

```
try:
    num_rows_deleted = db.session.query(Model).delete()
    db.session.commit()
except:
    db.session.rollback()
```

Delete All Records

```
#for all records
db.session.query(Model).delete()
db.session.commit()
```

Deleted Single Row

```
#for specific value
db.session.query(Model).filter(Model.id==123).delete()
db.session.commit()
```

Delete Single Record by Object

```
record_obj = db.session.query(Model).filter(Model.id==123).first()
db.session.delete(record_obj)
db.session.commit()
```
### 条件查询

https://www.jianshu.com/p/196b7892cf38








