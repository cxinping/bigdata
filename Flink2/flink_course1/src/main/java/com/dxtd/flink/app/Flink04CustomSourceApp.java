package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink04CustomSourceApp {

    /**
     * source
     * transformation
     * sink
     *
     * @param args
     */
    public static void main(String[] args) throws Exception {

        //构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(2);

        DataStream<VideoOrder2> videoOrderDS =  env.addSource(new VideoOrderSource());
        DataStream<VideoOrder2> filterDS = videoOrderDS.filter(new FilterFunction<VideoOrder2>() {
            @Override
            public boolean filter(VideoOrder2 videoOrder) throws Exception {
                return videoOrder.getMoney() > 5;
            }
        }).setParallelism(3);

        filterDS.print().setParallelism(4);

        //DataStream需要调用execute,可以取个名称
        env.execute("source job");
    }

}
