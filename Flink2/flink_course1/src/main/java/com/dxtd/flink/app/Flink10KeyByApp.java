package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 **/

public class Flink10KeyByApp {

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
//                new VideoOrder("542","springboot",45,5,new Date()),
//                new VideoOrder("42","redis",12,5,new Date()),
//                new VideoOrder("4252","java",32,5,new Date()),
//                new VideoOrder("42","springboot",45,5,new Date()),
//                new VideoOrder("554232","flink",30,5,new Date()),
//                new VideoOrder("23323","java",32,5,new Date())
//        );

        DataStreamSource<VideoOrder2> ds = env.addSource(new VideoOrderSourceV2());

        KeyedStream<VideoOrder2, String> videoOrderStringKeyedStream = ds.keyBy(new KeySelector<VideoOrder2, String>() {
            @Override
            public String getKey(VideoOrder2 value) throws Exception {
                return value.getTitle();
            }
        });


        SingleOutputStreamOperator<VideoOrder2> money = videoOrderStringKeyedStream.sum("money");

        money.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
