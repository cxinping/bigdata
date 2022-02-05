package net.xdclass.app;

import net.xdclass.model.VideoOrder;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Date;

/**
 *
 *
 **/

public class Flink13KeyByMaxByApp {

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
        //env.setParallelism(1);

        //数据源 source
        DataStream<VideoOrder> ds = env.fromElements(
                new VideoOrder("1","java",31,15,new Date()),
                new VideoOrder("2","java",32,45,new Date()),
                new VideoOrder("3","java",33,52,new Date()),
                new VideoOrder("4","springboot",21,5,new Date()),
                new VideoOrder("5","redis",41,52,new Date()),
                new VideoOrder("6","redis",40,15,new Date()),
                new VideoOrder("7","kafka",1,55,new Date())
        );

        KeyedStream<VideoOrder, String> keyByDS = ds.keyBy(new KeySelector<VideoOrder, String>() {
            @Override
            public String getKey(VideoOrder value) throws Exception {
                return value.getTitle();
            }
        });

        //SingleOutputStreamOperator<VideoOrder> money = keyByDS.max("money");
        SingleOutputStreamOperator<VideoOrder> money = keyByDS.maxBy("money");

        money.print();


        //DataStream需要调用execute,可以取个名称
        env.execute("map job");
    }

}
