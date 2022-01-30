package com.dxtd.flink.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Date;

/**
 *
 **/

public class Flink09FlatMapApp {

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

        DataStreamSource<String> ds = env.fromElements("spring,java", "springcloud,flink", "java,kafka");

        SingleOutputStreamOperator<String> flatMapDS = ds.flatMap(new RichFlatMapFunction<String, String>() {

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("========open");

            }

            @Override
            public void close() throws Exception {
                System.out.println("========close");
            }

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {

                String [] arr = value.split(",");
                for(String str:arr){
                    out.collect(str);
                }
            }
        });

        flatMapDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("flat map job");
    }

}