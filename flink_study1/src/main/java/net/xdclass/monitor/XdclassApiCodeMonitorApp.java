package net.xdclass.monitor;

import net.xdclass.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 *
 *
 **/

public class XdclassApiCodeMonitorApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<AccessLogDO> ds = env.addSource(new AccessLogSource());

        //过滤
        SingleOutputStreamOperator<AccessLogDO> filterDS = ds.filter(new FilterFunction<AccessLogDO>() {
            @Override
            public boolean filter(AccessLogDO value) throws Exception {
                return StringUtils.isNotBlank(value.getUrl());
            }
        });

        //指定watermark
        SingleOutputStreamOperator<AccessLogDO> watermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<AccessLogDO>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, timestamp) -> event.getCreateTime().getTime()));

        //最后兜底数据
        OutputTag<AccessLogDO> lateData = new OutputTag<AccessLogDO>("lateDataLog") {
        };


        //多个字段分组
        KeyedStream<AccessLogDO, Tuple2<String, Integer>> keyedStream = watermarkDS.keyBy(new KeySelector<AccessLogDO, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(AccessLogDO value) throws Exception {
                return Tuple2.of(value.getUrl(),value.getHttpCode());
            }
        });

        //开窗
        SingleOutputStreamOperator<ResultCount> aggregateDS = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateData)
                .aggregate(new AggregateFunction<AccessLogDO, Long, Long>() {

                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }

                    @Override
                    public Long add(AccessLogDO value, Long accumulator) {
                        return accumulator+1;
                    }

                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return a+b;
                    }
                }, new ProcessWindowFunction<Long, ResultCount, Tuple2<String, Integer>, TimeWindow>() {

                    @Override
                    public void process(Tuple2<String, Integer> value, Context context, Iterable<Long> elements, Collector<ResultCount> out) throws Exception {

                        ResultCount resultCount = new ResultCount();
                        resultCount.setUrl(value.f0);
                        resultCount.setHttpCode(value.f1);
                        long total = elements.iterator().next();
                        resultCount.setCount(total);
                        resultCount.setStartTime(TimeUtil.format(context.window().getStart()));
                        resultCount.setEndTime(TimeUtil.format(context.window().getEnd()));
                        out.collect(resultCount);
                    }
                });


        aggregateDS.print("接口状态码");

        aggregateDS.getSideOutput(lateData).print("late data");

        env.execute("XdclassMonitorApp");
    }
}
