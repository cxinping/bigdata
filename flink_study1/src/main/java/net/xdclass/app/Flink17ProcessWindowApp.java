package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import net.xdclass.source.VideoOrderSourceV2;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.stream.Collectors;

/**
 *
 *
 **/

public class Flink17ProcessWindowApp {

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

        SingleOutputStreamOperator<VideoOrder> process = keyByDS
                .window( TumblingProcessingTimeWindows.of(Time.seconds(5)))

                .process(new ProcessWindowFunction<VideoOrder, VideoOrder, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<VideoOrder> elements, Collector<VideoOrder> out) throws Exception {

                        List<VideoOrder> list = IteratorUtils.toList(elements.iterator());
                        int total = list.stream().collect(Collectors.summingInt(VideoOrder::getMoney)).intValue();
                        VideoOrder videoOrder = new VideoOrder();
                        videoOrder.setMoney(total);
                        videoOrder.setCreateTime(list.get(0).getCreateTime());
                        videoOrder.setTitle(list.get(0).getTitle());

                        out.collect(videoOrder);
                    }
                });
        process.print();

        //DataStream需要调用execute,可以取个名称
        env.execute("sliding window job");
    }

}
