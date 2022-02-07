package net.xdclass.project;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 *
 **/

public class Flink08KafkaSourceAppV02 {
    /**
     * source
     * transformation
     * sink
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {
        //构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 多并行度
        DataStream<UserLog> kafkaDS = env.addSource(new UserLogSource());
        kafkaDS.print("kafka:");

        //beginning
        Properties props = new Properties();
        //kafka地址
        props.setProperty("bootstrap.servers", "192.168.11.12:9092");
        //组名
        props.setProperty("group.id", "video-order-group");
        //字符串序列化和反序列化规则
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //offset重置规则
        props.setProperty("auto.offset.reset", "latest");
        //自动提交
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "2000");
        //有后台线程每隔10s检测一下Kafka的分区变化情况
        props.setProperty("flink.partition-discovery.interval-millis", "10000");

//       DataStream<String> mapDS = kafkaDS.map(new MapFunction<String, String>() {
//            @Override
//            public String map(String value) throws Exception {
//                return "王硕是精英："+value;
//            }
//        });
//
//        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("flink-result", new SimpleStringSchema(), props);
//
//        mapDS.addSink(producer);
//        // ending

        //DataStream需要调用execute,可以取个名称
        env.execute("kafka source job");
    }

}
