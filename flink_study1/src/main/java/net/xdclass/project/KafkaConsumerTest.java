package net.xdclass.project;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import java.util.Properties;

public class KafkaConsumerTest {
    /**

     /usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server  kafka1:9092  --replication-factor 1 --partitions 1 --topic sex

     /usr/local/kafka/bin/kafka-console-consumer.sh --topic sex --from-beginning --bootstrap-server kafka1:9092

     * */


    public static final String TOPIC_NAME = "sex";

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

    public void simpleConsumerTest(){
        Properties properties = getProperties();

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        //订阅主题
        kafkaConsumer.subscribe(Arrays.asList(KafkaProducerTest.TOPIC_NAME));

        while (true){
            //领取时间，阻塞超时时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : records){
                System.err.printf("topic=%s, offset=%d,key=%s,value=%s %n",record.topic(),record.offset(),record.key(),record.value());
            }

            //同步阻塞提交offset
            //kafkaConsumer.commitSync();

            if(!records.isEmpty()){
                //异步提交offset
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

                        if(exception == null){
                            System.err.println("手工提交offset成功:"+offsets.toString());
                        }else {
                            System.err.println("手工提交offset失败:"+offsets.toString());
                        }
                    }
                });
            }
        }

    }

    public void testConsumerDemo(){
        Properties properties = getProperties();

        KafkaConsumer<String,String> kafkaConsumer = new KafkaConsumer<>(properties);

        //订阅主题
        kafkaConsumer.subscribe(Arrays.asList(KafkaProducerTest.TOPIC_NAME));

        while (true){
            //领取时间，阻塞超时时间
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord record : records){
                System.err.printf("topic=%s, offset=%d,key=%s,value=%s %n",record.topic(),record.offset(),record.key(),record.value());
            }

            //同步阻塞提交offset
            //kafkaConsumer.commitSync();

            if(!records.isEmpty()){
                //异步提交offset
                kafkaConsumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {

                        if(exception == null){
                            System.err.println("手工提交offset成功:"+offsets.toString());
                        }else {
                            System.err.println("手工提交offset失败:"+offsets.toString());
                        }
                    }
                });
            }
        }

    }

    public static void main(String[] args) {
        KafkaConsumerTest consumer = new KafkaConsumerTest();
        //consumer.simpleConsumerTest();
        consumer.testConsumerDemo();


    }




}
