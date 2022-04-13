
### 打开 扩展名.ipynb是的文件

1，cmd下面输入

```
pip install jupyter notebook
```

2，cmd中输入以下命令，弹出一个页面先upload这个.ipynb后缀的文件，然后点击上传后的.ipynb文件

```
jupyter notebook
```

3, 转换游标查询的数据为字典

```
def dictfetchall(cursor):
     "Return all rows from a cursor as a dict"
     columns = [col[0] for col in cursor.description]
     return [
         dict(zip(columns, row))
         for row in cursor.fetchall()
     ]


```





