package com.flinkiot.study.course1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 使用Java API来开发Flink的 批处理应用程序.
 *
 */
public class BatchWCJavaApp {
    public static void main(String[] args) throws Exception {
        //String input = "file:///Users/rocky/tmp/flink/input";
        String input ="src/main/resources/input.txt";

        // step1：获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // step2：read data
        DataSource<String> text =  env.readTextFile(input);

        // step3: transform
        text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
               // System.out.println("value=" + value);
                String[] tokens = value.toLowerCase().split(",");

                for(String token : tokens) {
                    if(token.length() > 0) {
                        collector.collect(new Tuple2<String,Integer>(token.trim(),1));
                    }
                }
            }
        }).groupBy(0).sum(1).print();
    }

    
}

