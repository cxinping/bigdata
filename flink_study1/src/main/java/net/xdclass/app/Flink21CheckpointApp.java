package net.xdclass.app;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 *
 **/

public class Flink21CheckpointApp {

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

        //两个检查点之间间隔时间，默认是0,单位毫秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        //Checkpoint过程中出现错误，是否让整体任务都失败，默认值为0，表示不容忍任何Checkpoint失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(5);

        //Checkpoint是进行失败恢复，当一个 Flink 应用程序失败终止、人为取消等时，它的 Checkpoint 就会被清除
        //可以配置不同策略进行操作
        // DELETE_ON_CANCELLATION: 当作业取消时，Checkpoint 状态信息会被删除，因此取消任务后，不能从 Checkpoint 位置进行恢复任务
        // RETAIN_ON_CANCELLATION(多): 当作业手动取消时，将会保留作业的 Checkpoint 状态信息,要手动清除该作业的 Checkpoint 状态信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //Flink 默认提供 Extractly-Once 保证 State 的一致性，还提供了 Extractly-Once，At-Least-Once 两种模式，
        // 设置checkpoint的模式为EXACTLY_ONCE，也是默认的，
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        //设置checkpoint的超时时间, 如果规定时间没完成则放弃，默认是10分钟
        env.getCheckpointConfig().setCheckpointTimeout(50000);

        //设置同一时刻有多少个checkpoint可以同时执行，默认为1就行，以避免占用太多正常数据处理资源
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);


        //设置了重启策略, 作业在失败后能自动恢复,失败后最多重启3次，每次重启间隔10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));




        env.execute("watermark job");

    }

}
