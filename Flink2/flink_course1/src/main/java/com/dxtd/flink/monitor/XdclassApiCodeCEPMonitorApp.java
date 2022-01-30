package com.dxtd.flink.monitor;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


public class XdclassApiCodeCEPMonitorApp {

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
                WatermarkStrategy.<AccessLogDO>forMonotonousTimestamps()
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


        //定义好模式
        Pattern<AccessLogDO, AccessLogDO> pattern = Pattern.<AccessLogDO>begin("errorCode").where(new SimpleCondition<AccessLogDO>() {
            @Override
            public boolean filter(AccessLogDO value) throws Exception {
                return value.getHttpCode() != 200;
            }
        }).timesOrMore(3).within(Time.seconds(60));


        //匹配数据流
        PatternStream<AccessLogDO> patternStream = CEP.pattern(keyedStream, pattern);

        //获取结果
        SingleOutputStreamOperator<ResultCount> CEPResult = patternStream.select(new PatternSelectFunction<AccessLogDO, ResultCount>() {
            @Override
            public ResultCount select(Map<String, List<AccessLogDO>> map) throws Exception {

                List<AccessLogDO> list = map.get("errorCode");
                AccessLogDO accessLogDO = list.get(0);

                ResultCount resultCount = new ResultCount();
                resultCount.setUrl(accessLogDO.getUrl());
                resultCount.setCount(Long.valueOf(list.size()));
                resultCount.setHttpCode(accessLogDO.getHttpCode());
                return resultCount;
            }
        });

        CEPResult.print("接口告警");

        env.execute("XdclassMonitorCEPApp");

    }
}
