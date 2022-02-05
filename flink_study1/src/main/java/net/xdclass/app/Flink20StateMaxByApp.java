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
import org.apache.flink.configuration.Configuration;
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
 *
 *
 **/

public class Flink20StateMaxByApp {

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
        DataStream<String> ds = env.socketTextStream("127.0.0.1", 8888);


        DataStream<Tuple3<String, String, Integer>> flatMap = ds.flatMap(new FlatMapFunction<String, Tuple3<String, String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple3<String, String, Integer>> out) throws Exception {

                String[] arr = value.split(",");
                out.collect(Tuple3.of(arr[0], arr[1], Integer.parseInt(arr[2])));
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> maxVideoOrderDS = flatMap.keyBy(new KeySelector<Tuple3<String, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                return value.f0;
            }
        }).map(new RichMapFunction<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {

            private ValueState<Integer> maxVideoOrderState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                maxVideoOrderState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("maxValue",Integer.class));
            }

            @Override
            public Tuple2<String, Integer> map(Tuple3<String, String, Integer> value) throws Exception {
                //获取历史值

                Integer maxValue = maxVideoOrderState.value();

                Integer currentValue = value.f2;
                //判断
                if(maxValue == null || currentValue>maxValue){
                    //更新状态，当前最大的值存储到state
                    maxVideoOrderState.update(currentValue);
                    return Tuple2.of(value.f0,currentValue);
                }else {
                    return Tuple2.of(value.f0,maxValue);
                }
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });

        maxVideoOrderDS.print("最大订单：");



        env.execute("watermark job");

    }

}
