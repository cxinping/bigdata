package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.sink.VideoOrderCounterSink;
import net.xdclass.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

import java.util.Date;

/**
 *
 *
 **/

public class Flink09MapApp {

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
        DataStream<VideoOrder> ds = env.fromElements(
                new VideoOrder("21312","java",32,5,new Date()),
                new VideoOrder("314","java",32,5,new Date()),
                new VideoOrder("542","springboot",32,5,new Date()),
                new VideoOrder("42","redis",32,5,new Date()),
                new VideoOrder("4252","java",32,5,new Date()),
                new VideoOrder("42","springboot",32,5,new Date()),
                new VideoOrder("554232","flink",32,5,new Date()),
                new VideoOrder("23323","java",32,5,new Date())
        );



        //transformation
       DataStream<Tuple2<String,Integer>> mapDS =  ds.map(new RichMapFunction<VideoOrder, Tuple2<String,Integer>>() {

           @Override
           public void open(Configuration parameters) throws Exception {
               System.out.println("========open");
           }

           @Override
           public void close() throws Exception {
               System.out.println("========close");
           }

           @Override
            public Tuple2<String, Integer> map(VideoOrder value) throws Exception {
                return new Tuple2<>(value.getTitle(),1);
            }
        });


        mapDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
