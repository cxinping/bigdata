# 简介

安装手册

>  https://clickhouse.tech/#quick-start

ClickHouse 是一个用于联机分析(OLAP)的列式数据库管理系统(DBMS)。 



## 安装单机版ClickHouse 


在CentOS下安装单机版ClickHouse 



安装依赖 

```
yum install yum-utils
```



 确保CentOS支持SSE 

```
grep -q sse4_2 /proc/cpuinfo && echo "SSE 4.2 supported" || echo "SSE 4.2 not supported"
```



导入镜像源 

```
rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG

yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/clickhouse.repo
```



在线安装clickhouse的服务端和客户端 

```
yum install -y clickhouse-server clickhouse-client　　
```



如果在线安装实在太慢，可以自行下载rpm包进行安装 在 https://repo.yandex.ru/clickhouse/rpm/stable/x86_64/  先下载离线安装包

```
clickhouse-common-static-20.9.3.45-2.x86_64.rpm 
clickhouse-server-20.9.3.45-2.noarch.rpm  
clickhouse-client-20.9.3.45-2.noarch.rpm 
```

 注意，这三个rpm版本最好一致 , 注意安装顺序

```
rpm -ivh clickhouse-common-static-20.9.3.45-2.x86_64.rpm

rpm -ivh clickhouse-server-20.9.3.45-2.noarch.rpm  

rpm -ivh clickhouse-client-20.9.3.45-2.noarch.rpm 
```



启动服务 

```
systemctl start clickhouse-server
```



停止服务

```
systemctl stop clickhouse-server
```



 进入客户端 

```
clickhouse-client
```





## 设置系统参数 



CentOS 取消 打开文件数限制：

在/etc/security/limits.conf  /etc/security/limits.d/90-nproc.conf这两个文件最后新增以下内容：

```
* soft nofile 65536

* hard nofile 65536

* soft nproc 131072

* hard nproc 131072
```



## 远程连接配置项

vi /etc/clickhouse-server/config.xml中的如下配置项，类似mysql中的远程连接权限，放开ipv4连接打开注释 

> <listen_host>0.0.0.0</listen_host>

具体配置如下所示

```
<!-- Listen specified host. use :: (wildcard IPv6 address), if you want to accept connections both with IPv4 and IPv6 from everywhere. -->
<!-- <listen_host>::</listen_host> -->
<!-- Same for hosts with disabled ipv6: -->
 <listen_host>0.0.0.0</listen_host>

<!-- Default values - try listen localhost on ipv4 and ipv6: -->
<!--
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
-->
```



## 创建账号

1，clickhouse的密码有2种形式，一种是明文，一种是写sha256sum的Hash值

官方不建议直接写明文密码，可以用以下命令生成密码

```
PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
```

命令的返回值是

```
[root@localhost etc]# PASSWORD=$(base64 < /dev/urandom | head -c8); echo "$PASSWORD"; echo -n "$PASSWORD" | sha256sum | tr -d '-'
mCcteXsK
95f4ea9e80e5c679fbe4771f0aeb0209d9b88378e57bedc09fcc6ccfbd643e24  
```

mCcteXsK 是明文密码

95f4ea9e80e5c679fbe4771f0aeb0209d9b88378e57bedc09fcc6ccfbd643e24 是加密密码



2，cilckhouse的配置文件默认地址 /etc/clickhouse-server

```
cd /etc/clickhouse-server
```

修改 /etc/clickhouse-server/users.xml 文件， 找到 users --> default --> 标签下的password修改成password_sha256_hex，并把密文填进去 

```
<password_sha256_hex>密码密文</password_sha256_hex>
```



添加密码后，命令行启动的方式为

```
clickhouse-client -h ip地址 -d default -m -u default --password 明文密码
```

可以使用命令
```
clickhouse-client -h 127.0.0.1 -d default -m -u default --password mCcteXsK
```

用户名是default，密码是密码明文  mCcteXsK



3, 重启服务

```
systemctl restart clickhouse-server
```







# 常用操作



## 创建数据库



语法： CREATE DATABASE [IF NOT EXISTS] db_name 

如果数据库db_name已经存在，则不会创建新的db_name数据库



> create database if not exists spider



## 创建表



语法： CREATE TABLE t1(id UInt16,name String) ENGINE=TinyLog

例如： create table t1(id UInt8,name String,address String)engine=MergeTree order by id 



## 插入数据INSERT

语法：INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), …

例如：insert into t1 (id,name,address) values(1,'aa','addr1'),(2,'bb','addr2')




# 开发

## python连接clickhouse

 安装clickhouse驱动模块

```python
pip install clickhouse-driver
```



使用python操作clickhouse的表

```
from clickhouse_driver import Client

def get_clickhouse_client():
    host='192.168.11.129' #服务器地址
    port = 9000 #端口
    user= 'default' #用户名
    password= 'mCcteXsK' #密码
    database= 'spider' #数据库
    send_receive_timeout = 5 #超时时间
    client = Client(host=host, port=port, user=user, password=password,database=database, send_receive_timeout=send_receive_timeout)
    return client

def insert_demo():
    client = get_clickhouse_client()
    client.execute("""insert into t1(name) values('wangwu-111')""")
```





# MySQL表引擎



MySQL引擎用于将远程的MySQL服务器中的表映射到ClickHouse中，并允许您对表进行insert和select查询，以方便您在ClickHouse与MySQL之间进行数据交换。
MySQL数据库引擎会将对其的查询转换为MySQL语法并发送到MySQL服务器中，因此您可以执行诸如show tables或show create table之类的操作。



创建一张MySQL测试表

```
DROP TABLE IF EXISTS `mysql_engine`;

CREATE TABLE `mysql_engine` (
  `id` int(10) NOT NULL AUTO_INCREMENT,
  `user_name` varchar(255) CHARACTER SET utf8 DEFAULT NULL,
  `createDate` datetime default now() comment '创建时间' ,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1000 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='测试表';
```



 创建clickhouse表，并指定引擎为mysql 

```
create table remote_mysql_engine
(
    id     Int32,
    user_name String,
    createDate DateTime
)
    engine = MySQL('192.168.11.129:3306', 'spider', 'mysql_engine', 'root', 'root');
```







# 参考资料：

https://www.cnblogs.com/tencentdb/p/13915001.html

https://blog.csdn.net/weixin_39025362/article/details/109165055




























