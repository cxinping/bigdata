package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.source.VideoOrderSource;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.configuration.Configuration;
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

public class Flink05Sink1App {

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
        env.setParallelism(2);

        DataStream<VideoOrder> videoOrderDS =  env.addSource(new VideoOrderSource());

        DataStream<VideoOrder> filterDS = videoOrderDS.filter(new FilterFunction<VideoOrder>() {
            @Override
            public boolean filter(VideoOrder videoOrder) throws Exception {
                return videoOrder.getMoney()>5;
            }
        });

        filterDS.printToErr();
        filterDS.print();


        //DataStream需要调用execute,可以取个名称
        env.execute("sink job");
    }

}
