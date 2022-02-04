# Sink 例子

```aidl
create table remote_mysql_engine
(
id     Int32,
user_name String,
createDate DateTime
)
```

# 地图

高德地图 IP定位

> https://lbs.amap.com/api/webservice/guide/api/ipconfig

# 项目的架构图

> Kafka Source ==> Flink ==> ClickHouse(大宽表) <== SQL

架构没有对错直说，关键在于是否合适你公司的业务场景










