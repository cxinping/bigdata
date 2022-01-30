package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 *
 **/

public class Flink11FilterApp {

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

        DataStreamSource<VideoOrder2> ds = env.addSource(new VideoOrderSourceV2());


        SingleOutputStreamOperator<VideoOrder2> filterDS = ds.filter(new FilterFunction<VideoOrder2>() {
            @Override
            public boolean filter(VideoOrder2 value) throws Exception {
                return value.getMoney()>20;
            }
        });

        KeyedStream<VideoOrder2, String> videoOrderStringKeyedStream = filterDS.keyBy(new KeySelector<VideoOrder2, String>() {
            @Override
            public String getKey(VideoOrder2 value) throws Exception {
                return value.getTitle();
            }
        });

        SingleOutputStreamOperator<VideoOrder2> sumDS = videoOrderStringKeyedStream.sum("money");
        sumDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
