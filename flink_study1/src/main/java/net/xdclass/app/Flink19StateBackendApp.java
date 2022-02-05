package net.xdclass.app;

import net.xdclass.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description 流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class Flink19StateBackendApp {

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
        env.setParallelism(1);
        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage(new JobManagerCheckpointStorage());
        //env.setStateBackend(new EmbeddedRocksDBStateBackend());

        //env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint-dir");
        //env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///checkpoint-dir"));
        //作业单独配置checkpoints
        //env.getCheckpointConfig().setCheckpointStorage("hdfs:///checkpoints-data/");
        //java,2022-11-11 09-10-10,15
        DataStream<String> ds = env.socketTextStream("127.0.0.1", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMapDS = ds.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));

            }
        });

        flatMapDS.print();
        env.execute("watermark job");

    }

}
