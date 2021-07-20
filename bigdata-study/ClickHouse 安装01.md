# 简介

安装手册

>  https://clickhouse.tech/#quick-start



 ClickHouse 是一个用于联机分析(OLAP)的列式数据库管理系统(DBMS)。 





## 下载

在线安装

```
sudo yum install yum-utils
sudo rpm --import https://repo.clickhouse.tech/CLICKHOUSE-KEY.GPG
sudo yum-config-manager --add-repo https://repo.clickhouse.tech/rpm/clickhouse.repo
sudo yum install clickhouse-server clickhouse-client

sudo /etc/init.d/clickhouse-server start clickhouse-client
```



离线安装

































