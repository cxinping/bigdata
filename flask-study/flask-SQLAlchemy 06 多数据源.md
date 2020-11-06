http://www.pythondoc.com/flask-sqlalchemy/binds.html

```
class TableName(db.Model):
     __tablename__ = 'tablename'
    __bind_key__ = 'db1' #在此处执行即可
```