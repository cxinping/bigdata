package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder2;
import com.dxtd.flink.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink17AggWindowApp {

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

        SingleOutputStreamOperator<VideoOrder2> aggregate = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<VideoOrder2, VideoOrder2, VideoOrder2>() {
                    //初始化累加器
                    @Override
                    public VideoOrder2 createAccumulator() {
                        VideoOrder2 videoOrder = new VideoOrder2();
                        return videoOrder;
                    }

                    //聚合操作
                    @Override
                    public VideoOrder2 add(VideoOrder2 value, VideoOrder2 accumulator) {

                        accumulator.setMoney(value.getMoney() + accumulator.getMoney());

                        if (accumulator.getTitle() == null) {
                            accumulator.setTitle(value.getTitle());
                        }
                        if (accumulator.getCreateTime() == null) {
                            accumulator.setCreateTime(value.getCreateTime());
                        }
                        return accumulator;
                    }

                    //获取结果
                    @Override
                    public VideoOrder2 getResult(VideoOrder2 accumulator) {
                        return accumulator;
                    }

                    @Override
                    public VideoOrder2 merge(VideoOrder2 a, VideoOrder2 b) {
                        VideoOrder2 videoOrder = new VideoOrder2();
                        videoOrder.setMoney(a.getMoney() + b.getMoney());
                        videoOrder.setTitle(a.getTitle());
                        return videoOrder;
                    }
                });

        aggregate.print();


        //DataStream需要调用execute,可以取个名称
        env.execute("sliding window job");
    }

}
