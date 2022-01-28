package com.dxtd.flink.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * $ sudo nc -l 8888
 *
 * */
public class WebUIApp {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setParallelism(1);

        DataStream<String> stringDataStream = env.socketTextStream("192.168.11.12",8888);
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
        //flatMapDataStream.print("");

        //DataStream需要调用execute,可以取个名称
        env.execute("data stream job");

    }
}
