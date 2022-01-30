package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.sink.MysqlSink;
import com.dxtd.flink.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description 流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class Flink06CustomMysqSinkApp {

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

        DataStream<VideoOrder2> videoOrderDS =  env.addSource(new VideoOrderSource());

        DataStream<VideoOrder2> filterDS = videoOrderDS.filter(new FilterFunction<VideoOrder2>() {
            @Override
            public boolean filter(VideoOrder2 videoOrder) throws Exception {
                return videoOrder.getMoney() > 50;
            }
        });

        filterDS.print();

        filterDS.addSink(new MysqlSink());

        //DataStream需要调用execute,可以取个名称
        env.execute("custom mysql sink job");
    }

}
