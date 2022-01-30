package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink16CountWindowApp {

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

        //数据源 source
        DataStream<VideoOrder2> ds = env.addSource(new VideoOrderSourceV2());

        KeyedStream<VideoOrder2, String> keyByDS = ds.keyBy(new KeySelector<VideoOrder2, String>() {
            @Override
            public String getKey(VideoOrder2 value) throws Exception {
                return value.getTitle();
            }
        });

        //分组后的组内数据超过5个则触发
        //DataStream<VideoOrder> sumDS = keyByDS.countWindow(5).sum("money");

        //分组后的组内数据超过3个则触发统计过去的5个数据
        DataStream<VideoOrder2> sumDS = keyByDS.countWindow(5,3).sum("money");

        sumDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("sliding window job");
    }

}
