package net.xdclass.project;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Stream;

public class KafkaProducerTest {
    /**
     *
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

    public void testSendDemo() throws IOException {
        Properties properties = getProperties();
        Producer<String,String> producer = new KafkaProducer<>(properties);

        String fileName = "data/user_log1.csv";
        File file = new File(fileName);
        // 读取文件内容到Stream流中，按行读取
        Stream<String> lines = Files.lines(Paths.get(fileName));

        lines.forEachOrdered(ele -> {
            String[] line =ele.split(",");
            String user_id = line[0];

            if( !user_id.equals("user_id")) { //买家id在每行日志代码的第0个元素,去除第一行表头
                System.out.println(ele);
//                UserLog userLog = new UserLog();
//                userLog.setUser_id(line[0]);
//                userLog.setItem_id(line[1]);
//                userLog.setCat_id(line[2]);
//                userLog.setMonth(line[5]);
//                userLog.setDay(line[6]);
//                userLog.setGender(line[9]);
//                userLog.setProvince(line[10]);

                try {
                    // 每隔0.1秒发送一行数据, 1秒10条记录
                    long sleep_time = Double.valueOf(0.1 * 1000).longValue() ;
                    Thread.sleep(sleep_time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                producer.send(new ProducerRecord<>(TOPIC_NAME,line[9]));
            }

        });

        producer.close();

    }

    public static void main(String[] args) throws IOException {
        KafkaProducerTest producer = new KafkaProducerTest();
        //producer.testSend();
        producer.testSendDemo();

    }

}
