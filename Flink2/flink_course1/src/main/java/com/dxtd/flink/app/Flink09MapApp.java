package com.dxtd.flink.app;


import com.dxtd.flink.model.VideoOrder2;
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
        DataStream<VideoOrder2> ds = env.fromElements(
                new VideoOrder2("21312","java",32,5,new Date()),
                new VideoOrder2("314","java",32,5,new Date()),
                new VideoOrder2("542","springboot",32,5,new Date()),
                new VideoOrder2("42","redis",32,5,new Date()),
                new VideoOrder2("4252","java",32,5,new Date()),
                new VideoOrder2("42","springboot",32,5,new Date()),
                new VideoOrder2("554232","flink",32,5,new Date()),
                new VideoOrder2("23323","java",32,5,new Date())
        );

        //transformation
       DataStream<Tuple2<String,Integer>> mapDS =  ds.map(new RichMapFunction<VideoOrder2, Tuple2<String,Integer>>() {

           @Override
           public void open(Configuration parameters) throws Exception {
               System.out.println("========open");
           }

           @Override
           public void close() throws Exception {
               System.out.println("========close");
           }

           @Override
            public Tuple2<String, Integer> map(VideoOrder2 value) throws Exception {
                return new Tuple2<>(value.getTitle(),1);
            }
        });


        mapDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
