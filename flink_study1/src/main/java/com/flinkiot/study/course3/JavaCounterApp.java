package com.flinkiot.study.course3;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

/**
 * Author: Michael PK
 */
public class JavaCounterApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> data = env.fromElements("hadoop","spark","flink","pyspark","storm");

        DataSet<String> info = data.map(new RichMapFunction<String, String>() {

            LongCounter counter = new LongCounter();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                getRuntimeContext().addAccumulator("ele-counts-java", counter);
            }

            public String map(String value) throws Exception {
                counter.add(1);
                return value;
            }
        });

        String filePath = "file:///Users/rocky/IdeaProjects/imooc-workspace/data/04/sink-java-counter-out/";
        info.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult jobResult = env.execute("CounterApp");

        // step3: 获取计数器
        long num = jobResult.getAccumulatorResult("ele-counts-java");

        System.out.println("num = [" + num + "]");
    }
}
