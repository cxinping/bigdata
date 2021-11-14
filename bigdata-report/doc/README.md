
欢迎页面
> http://127.0.0.1:8004/hello

test url
> http://127.0.0.1:8004/report/test/abc

> http://10.5.138.11:8004/report/test/abc

> http://10.5.138.11:8004/report/test/abc


docker
> http://10.5.138.11:8004/hello

> http://10.5.138.11:8004/report/test/abc

> http://192.168.11.130:8004/hello

> http://192.168.11.130:8004/report/test/abc

# MySQL 数据库

区域表
```
create database report default character set utf8 collate utf8_general_ci;

drop table areas;

create table areas(
   id VARCHAR (50) NOT NULL COMMENT '主键',
   area_name    VARCHAR (200)    comment "区域名称",
   city         VARCHAR (200)    comment "区域所在市",
   province     VARCHAR (200)    comment "区域所在省",
   primary key(id)
) ENGINE = INNODB DEFAULT CHARSET = utf8;

```  


字符集乱码
```
SET CHARACTER_SET_RESULTS=utf8;

```


插入操作或更新操作 on duplicate
```
insert into T_name (uid, app_id,createTime,modifyTime) 
values(111, 1000000,'2017-03-07 10:19:12','2017-03-07 10:19:12') 
on duplicate key update uid=111, app_id=1000000, createTime='2017-03-07 10:19:12',modifyTime='2017-05-07 10:19:12'

INSERT INTO areas(area_name, city, province) on duplicate key update area_name= , city= , province=

```

运行 Report 工程

```
cd /you_filed_algos/app

/root/anaconda3/bin/python /you_filed_algos/app/run.py

nohup /root/anaconda3/bin/python /you_filed_algos/app/run.py &

```

查看多线程的状态
```
ps -eLf | grep python
```

jieba 词典
```
https://github.com/fxsjy/jieba
```







