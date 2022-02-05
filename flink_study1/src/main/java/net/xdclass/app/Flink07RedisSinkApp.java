package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.sink.MysqlSink;
import net.xdclass.sink.VideoOrderCounterSink;
import net.xdclass.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description 流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class Flink07RedisSinkApp {

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
        env.setParallelism(1);

        //数据源 source
//        DataStream<VideoOrder> ds = env.fromElements(
//                new VideoOrder("21312","java",32,5,new Date()),
//                new VideoOrder("314","java",32,5,new Date()),
//                new VideoOrder("542","springboot",32,5,new Date()),
//                new VideoOrder("42","redis",32,5,new Date()),
//                new VideoOrder("4252","java",32,5,new Date()),
//                new VideoOrder("42","springboot",32,5,new Date()),
//                new VideoOrder("554232","flink",32,5,new Date()),
//                new VideoOrder("23323","java",32,5,new Date())
//        );
        DataStream<VideoOrder> ds = env.addSource(new VideoOrderSource());



        //transformation
       DataStream<Tuple2<String,Integer>> mapDS =  ds.map(new MapFunction<VideoOrder, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(),1);
            }
        });



//        DataStream<Tuple2<String,Integer>> mapDS = ds.flatMap(new FlatMapFunction<VideoOrder, Tuple2<String,Integer>>() {
//            @Override
//            public void flatMap(VideoOrder value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                out.collect(new Tuple2<>(value.getTitle(),1));
//            }
//        });


       //分组
        KeyedStream<Tuple2<String,Integer>,String> keyByDS = mapDS.keyBy(new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //统计每组有多少个
        DataStream<Tuple2<String,Integer>> sumDS =  keyByDS.sum(1);

        //控制台打印
        sumDS.print();

        //单机redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).build();

        sumDS.addSink(new RedisSink<>(conf,new VideoOrderCounterSink()));


        //DataStream需要调用execute,可以取个名称
        env.execute("custom redis sink job");
    }

}
