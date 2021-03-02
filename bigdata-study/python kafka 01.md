# 使用python操作kafka

使用python操作kafka目前比较常用的库是kafka-python库

## 安装kafka-python
```
pip3 install kafka-python
```

## 生产者
producer_test.py

```
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='192.168.0.121:9092')  # 连接kafka

msg = "Hello World".encode('utf-8')  # 发送内容,必须是bytes类型
producer.send('test', msg)  # 发送的topic为test
producer.close()
```

## 消费者
consumer_test.py

```
from kafka import KafkaConsumer

consumer = KafkaConsumer('test', bootstrap_servers=['192.168.0.121:9092'])
for msg in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (msg.topic, msg.partition, msg.offset, msg.key, msg.value)
    print(recv)
```

执行此程序，此时会hold住，因为它在等待生产者发送消息！




# 参考资料

> https://github.com/dpkp/kafka-python

hello world kafuka
> https://timber.io/blog/hello-world-in-kafka-using-python/




