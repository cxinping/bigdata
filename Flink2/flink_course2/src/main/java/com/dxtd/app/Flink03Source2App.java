package com.dxtd.app;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


public class Flink03Source2App {
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
        //env.setParallelism(1);
        //  DataStream<String> ds = env.readTextFile("/Users/xdclass/Desktop/xdclass_access.log");
        //  DataStream<String> textDS = env.readTextFile("hdfs://xdclass_node:8010/file/log/words.txt");
        DataStream<String> stringDataStream = env.socketTextStream("127.0.0.1", 8888);
        stringDataStream.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("source job");
    }

}
