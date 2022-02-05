package net.xdclass.app;

import net.xdclass.util.TimeUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 *
 *
 **/
public class Flink22CEPLoginApp {

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

        //AA,2022-11-11 12:01:01,-1
        DataStream<String> ds = env.socketTextStream("127.0.0.1", 8888);


        DataStream<Tuple3<String, String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {

                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        });

        //指定event time列
        SingleOutputStreamOperator<Tuple3<String, String, Integer>> watermarkDS = flatMap.assignTimestampsAndWatermarks(WatermarkStrategy
                //生成3秒延迟的watermark
                //.<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(3))

                //时间是单调递增，event time 充当了水印
                .<Tuple3<String, String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner((event, timestamp) -> TimeUtil.strToDate(event.f1).getTime()));

        //分组
        KeyedStream<Tuple3<String, String, Integer>, String> keyedStream = watermarkDS.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        });


        //cep 1、定义模式pattern
        Pattern<Tuple3<String, String, Integer>, Tuple3<String, String, Integer>> pattern = Pattern

                .<Tuple3<String, String, Integer>>begin("firstLoginTime")

                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        // -1 是登录失败的错误码
                        return value.f2 == -1;
                    }
                })
                .next("secondLoginTime")
                .where(new SimpleCondition<Tuple3<String, String, Integer>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, Integer> value) throws Exception {
                        // -1 是登录失败的错误码
                        return value.f2 == -1;
                    }
                })
                .within(Time.seconds(5));


        //cep 2、匹配数据流
        PatternStream<Tuple3<String, String, Integer>> patternStream = CEP.pattern(keyedStream, pattern);

        //cep 3、获取结果
        SingleOutputStreamOperator<Tuple3<String, String, String>> select = patternStream.select(new PatternSelectFunction<Tuple3<String, String, Integer>, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> select(Map<String, List<Tuple3<String, String, Integer>>> map) throws Exception {

                Tuple3<String, String, Integer> firstLoginTimeEvent = map.get("firstLoginTime").get(0);

                Tuple3<String, String, Integer> secondLoginTimeEvent = map.get("secondLoginTime").get(0);

                return Tuple3.of(firstLoginTimeEvent.f0,firstLoginTimeEvent.f1,secondLoginTimeEvent.f1);
            }
        });

        select.print("风险账号");

        env.execute("cep job");

    }

}
