package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description 流处理
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

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
        DataStream<VideoOrder> ds = env.addSource(new VideoOrderSourceV2());

        KeyedStream<VideoOrder, String> keyByDS = ds.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        });

        SingleOutputStreamOperator<VideoOrder> aggregate = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(new AggregateFunction<VideoOrder, VideoOrder, VideoOrder>() {


                    //初始化累加器
                    @Override
                    public VideoOrder createAccumulator() {
                        VideoOrder videoOrder = new VideoOrder();
                        return videoOrder;
                    }

                    //聚合操作
                    @Override
                    public VideoOrder add(VideoOrder value, VideoOrder accumulator) {

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
                    public VideoOrder getResult(VideoOrder accumulator) {
                        return accumulator;
                    }

                    @Override
                    public VideoOrder merge(VideoOrder a, VideoOrder b) {
                        VideoOrder videoOrder = new VideoOrder();
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
