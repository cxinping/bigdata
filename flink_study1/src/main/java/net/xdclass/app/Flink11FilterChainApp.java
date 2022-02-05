package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description 流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class Flink11FilterChainApp {

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

        DataStreamSource<VideoOrder> ds = env.addSource(new VideoOrderSourceV2());


        DataStream<VideoOrder> sumDS = ds.filter(new FilterFunction<VideoOrder>() {
            @Override
            public boolean filter(VideoOrder value) throws Exception {
                return value.getMoney() > 20;
            }
        }).keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        }).sum("money");


        sumDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
