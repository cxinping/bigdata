package net.xdclass.monitor;

import net.xdclass.util.TimeUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
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
public class XdclassMonitorApp {
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


        //分组
        KeyedStream<AccessLogDO, String> keyedStream = watermarkDS.keyBy(new KeySelector<AccessLogDO, String>() {
            @Override
            public String getKey(AccessLogDO value) throws Exception {
                return value.getUrl();
            }
        });


        //开窗
        WindowedStream<AccessLogDO, String, TimeWindow> windowedStream = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(60), Time.seconds(5)))
                //允许有1分钟延迟
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(lateData);


        SingleOutputStreamOperator<ResultCount> aggregate = windowedStream.aggregate(new AggregateFunction<AccessLogDO, Long, Long>() {
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
        }, new ProcessWindowFunction<Long, ResultCount, String, TimeWindow>() {

            @Override
            public void process(String value, Context context, Iterable<Long> elements, Collector<ResultCount> out) throws Exception {

                ResultCount resultCount = new ResultCount();
                resultCount.setUrl(value);
                resultCount.setStartTime(TimeUtil.format(context.window().getStart()));
                resultCount.setEndTime(TimeUtil.format(context.window().getEnd()));
                long total = elements.iterator().next();
                resultCount.setCount(total);
                out.collect(resultCount);
            }
        });

        aggregate.print("实时1分钟接口访问量");

        env.execute("XdclassMonitorApp");

    }
}
