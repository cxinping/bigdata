package com.ultrapower.flink.source;

import com.ultrapower.flink.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;
import java.util.Properties;

/**
 *
 *
 * */
public class SourceApp {

    public static void main(String[] args) throws Exception {
        // 创建上下文
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

       // test01(env);
      //  test02(env);
       // test03(env);
        test04(env);
         //test05(env);

        env.execute("SourceApp");
    }

    public static void test05(StreamExecutionEnvironment env) {
        /**
         /usr/local/kafka/bin/kafka-topics.sh --create --bootstrap-server  kafka1:9092  --replication-factor 1 --partitions 1 --topic flinktopic

         * */
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.11.12:9092");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<>("flinktopic", new SimpleStringSchema(), properties));

        System.out.println("* stream.getParallelism()="+stream.getParallelism());
        stream.print();
    }

    public static void test04(StreamExecutionEnvironment env) {
        DataStreamSource<Student> source = env.addSource(new StudentSource()).setParallelism(1);
        System.out.println(source.getParallelism());
        source.print();
    }

    public static void test03(StreamExecutionEnvironment env) {
        // 单并行度
        //DataStreamSource<Access> source = env.addSource(new AccessSource()).setParallelism(1);

        // 多并行度
        DataStreamSource<Access> source = env.addSource(new AccessSourceV2()).setParallelism(3);
        System.out.println("*** Parallelism=>"+source.getParallelism());
        source.print("source");
    }

    public static void test02(StreamExecutionEnvironment env) {
        //env.setParallelism(5); // 对于env设置的并行度 是一个全局的概念

        DataStreamSource<Long> source = env.fromParallelCollection(
                new NumberSequenceIterator(1, 10), Long.class
        ).setParallelism(4);   // 这是局部的概念

        System.out.println("source:" + source.getParallelism());

        SingleOutputStreamOperator<Long> filterStream = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value >= 5;
            }
        }).setParallelism(3); // 对于算子层面的并行度，如果全局设置，以本算子的并行度为准
        System.out.println("filterStream:" + filterStream.getParallelism());

        filterStream.print("filter");
    }

    public static void test01(StreamExecutionEnvironment env) {
        /**
         * $ sudo nc -l 9527
         *
         * */
        //env.setParallelism(5);

//        StreamExecutionEnvironment.createLocalEnvironment();
//        StreamExecutionEnvironment.createLocalEnvironment(3);
//        StreamExecutionEnvironment.createLocalEnvironment(new Configuration());
//        StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
//        StreamExecutionEnvironment.createRemoteEnvironment();

        DataStreamSource<String> source = env.socketTextStream("192.168.11.12", 9527);
        System.out.println("source...." + source.getParallelism()); // ?  1

        // 接收socket过来的数据，一行一个单词， 把pk的过滤掉
        SingleOutputStreamOperator<String> filterStream = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"pk".equals(value);
            }
        });//.setParallelism(6);

        // 打印当前CPU的数量
        System.out.println("filter...." + filterStream.getParallelism());
        filterStream.print("filter");
    }

}
