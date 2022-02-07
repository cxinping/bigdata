package net.xdclass.project;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaProducerTest {
    /**
     *
     * */
    public static final String TOPIC_NAME = "test2";

    public Properties getProperties(){
        Properties props = new Properties();
        //kafka地址
        props.setProperty("bootstrap.servers", "192.168.11.12:9092");
        //组名
        props.setProperty("group.id", "video-order-group");
        //字符串序列化和反序列化规则
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        /**
         * key的序列化器，将用户提供的 key和value对象ProducerRecord 进行序列化处理，key.serializer必须被设置，
         * 即使消息中没有指定key，序列化器必须是一个实
         org.apache.kafka.common.serialization.Serializer接口的类，
         * 将key序列化成字节数组。
         */
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        //offset重置规则
        props.setProperty("auto.offset.reset", "latest");
        //自动提交
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "2000");
        //有后台线程每隔10s检测一下Kafka的分区变化情况
        props.setProperty("flink.partition-discovery.interval-millis", "10000");

        return props;
    }

    public void testSend(){
        Properties properties = getProperties();

        Producer<String,String> producer = new KafkaProducer<>(properties);

        for(int i=0;i<3 ;i++){
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(TOPIC_NAME,"flinkiot-key"+i, "flinkiot-value"+i));

            try {
                //不关心结果则不用写这些内容
                RecordMetadata recordMetadata =  future.get();
                // topic - 分区编号@offset
                System.out.println("发送状态："+recordMetadata.toString());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }

        producer.close();
    }

    public static void main(String[] args) {
        KafkaProducerTest producer = new KafkaProducerTest();
        producer.testSend();
    }
}
