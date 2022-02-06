package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.source.VideoOrderSourceV2;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 *
 *
 **/

public class Flink14TumblingWindowApp {

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

        DataStream<VideoOrder> sumDS = keyByDS.window(TumblingProcessingTimeWindows.of(Time.seconds(5))).sum("money");

        sumDS.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("tumbling window job");
    }

}
