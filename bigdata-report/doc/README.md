
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


```
create database report default character set utf8 collate utf8_general_ci;


create table areas(
   id INT (12) NOT NULL AUTO_INCREMENT COMMENT '自增主键',
   area_name    VARCHAR (200)     comment "区域名称",
   city         VARCHAR (200)    comment "区域所在市",
   province     VARCHAR (200)     comment "区域所在省",
   primary key(id)
) ENGINE = INNODB DEFAULT CHARSET = utf8;
```















