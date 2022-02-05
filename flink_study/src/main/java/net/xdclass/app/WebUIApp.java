package net.xdclass.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class WebUIApp {

    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStream<String> stringDataStream = env.socketTextStream("127.0.0.1",8888);


        DataStream<String> flatMapDataStream = stringDataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                String[] arr = value.split(",");
                for (String word : arr) {
                    out.collect(word);
                }
            }
        });
        flatMapDataStream.print("结果");

        //DataStream需要调用execute,可以取个名称
        env.execute("data stream job");

    }
}
