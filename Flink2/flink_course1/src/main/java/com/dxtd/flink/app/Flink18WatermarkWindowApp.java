package com.dxtd.flink.app;

import com.dxtd.flink.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
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
 * $ sudo nc -l 8888
 *
 * */
public class Flink18WatermarkWindowApp {

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

        //java,2022-11-11 09-10-10,15
        DataStream<String> ds = env.socketTextStream("192.168.11.12", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> flatMapDS = ds.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));

            }
        });

        //指定watermark
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> watermarkDS = flatMapDS.assignTimestampsAndWatermarks(WatermarkStrategy
                //指定允许乱序延迟的最大时间 3 秒
                .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                //指定POJO事件时间列，毫秒
                .withTimestampAssigner((event, timestamp) -> TimeUtil.strToDate(event.f1).getTime()));

        //最后的兜底数据
        OutputTag<Tuple3<String, String, Integer>> lateData = new OutputTag<Tuple3<String, String, Integer>>("lateDataOrder"){};

        //分组 开窗
        SingleOutputStreamOperator<String> sumDS = watermarkDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        })
                //开窗
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))

                //允许1分钟延迟
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateData)

                //聚合， 方便调试拿到窗口全部数据，全窗口函数
                .apply(new WindowFunction<Tuple3<String, String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window, Iterable<Tuple3<String, String, Integer>> input, Collector<String> out) throws Exception {

                        //准备list，存储窗口的事件时间
                        List<String> timeList = new ArrayList<>();
                        int total = 0;
                        for(Tuple3<String, String, Integer> order:input){
                            timeList.add(order.f1);
                            total = total+order.f2;
                        }

                        String outStr = String.format("分组key:%s,聚合值:%s,窗口开始结束:[%s~%s),窗口所有事件时间:%s", key,total, TimeUtil.format(window.getStart()),TimeUtil.format(window.getEnd()), timeList);
                        out.collect(outStr);

                    }
                });

        sumDS.print();

        //最后兜底处理，更新之前的数据，小滴课堂 海量数据处理项目大课
        sumDS.getSideOutput(lateData).print("late data order");

        env.execute("watermark job");

    }

}
