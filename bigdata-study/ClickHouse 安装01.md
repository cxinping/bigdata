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


































