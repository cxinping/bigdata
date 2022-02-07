package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.sink.VideoOrderCounterSink;
import net.xdclass.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Properties;

/**
 *
 *
 **/

public class Flink08KafkaSourceApp {

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
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        Properties props = new Properties();
        //kafka地址
        props.setProperty("bootstrap.servers", "localhost:9092");
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
        props.setProperty("flink.partition-discovery.interval-millis","10000");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("xdclass-topic", new SimpleStringSchema(),props);

        //设置从记录的消费者组内的offset开始消费
        consumer.setStartFromGroupOffsets();

        DataStream<String> kafkaDS = env.addSource(consumer);

        kafkaDS.print("kafka:");

       DataStream<String> mapDS = kafkaDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "小滴课堂："+value;
            }
        });

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("xdclass-order", new SimpleStringSchema(), props);

        mapDS.addSink(producer);

        //DataStream需要调用execute,可以取个名称
        env.execute("kafka source job");
    }

}
