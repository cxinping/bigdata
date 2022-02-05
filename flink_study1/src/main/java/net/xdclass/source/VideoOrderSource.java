package net.xdclass.source;

import net.xdclass.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 *
 *
 **/

public class VideoOrderSource extends RichParallelSourceFunction<VideoOrder> {
    private volatile Boolean flag = true;

    private Random random = new Random();

    private static List<String> list = new ArrayList<>();
    static {
        list.add("spring boot2.x课程");
        list.add("微服务SpringCloud课程");
        list.add("RabbitMQ消息队列");
        list.add("Kafka课程");
        list.add("小滴课堂面试专题第一季");
        list.add("Flink流式技术课程");
        list.add("工业级微服务项目大课训练营");
        list.add("Linux课程");
    }

    /**
     * run 方法调用前 用于初始化连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("-----open-----");
    }

    /**
     * 用于清理之前
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("-----close-----");
    }

    /**
     * 产生数据的逻辑
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<VideoOrder> ctx) throws Exception {

        while (flag){
            Thread.sleep(1000);
            String id = UUID.randomUUID().toString();
            int userId = random.nextInt(10);
            int money = random.nextInt(100);
            int videoNum = random.nextInt(list.size());
            String title = list.get(videoNum);
            VideoOrder videoOrder = new VideoOrder(id,title,money,userId,new Date());

            ctx.collect(videoOrder);
        }
    }

    /**
     * 控制任务取消
     */
    @Override
    public void cancel() {

        flag = false;
    }
}
