package net.xdclass.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description  流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class Flink03Source1App {
    /**
     * source
     * transformation
     * sink
     * @param args
     */
    public static void main(String [] args) throws Exception {
        //构建执行任务环境以及任务的启动的入口, 存储全局相关的参数
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //相同类型元素的数据流 source
        //DataStream<String> ds1 = env.fromElements("java,SpringBoot", "spring cloud,redis", "kafka,小滴课堂");
        //ds1.print("ds1:");

        //相同类型元素的数据流 source
        //DataStream<String> ds2 = env.fromCollection(Arrays.asList("java,SpringBoot", "spring cloud,redis", "kafka,小滴课堂"));
        //ds2.print("ds2:");

        DataStream<Long> ds3 = env.fromSequence(1,10);
        ds3.print("ds3:");

        //DataStream需要调用execute,可以取个名称
        env.execute("source job");
    }

}
