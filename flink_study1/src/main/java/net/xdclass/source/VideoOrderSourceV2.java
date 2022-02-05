package net.xdclass.source;

import net.xdclass.model.VideoOrder;
import net.xdclass.util.TimeUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.*;

/**
 *
 *
 **/

public class VideoOrderSourceV2 extends RichParallelSourceFunction<VideoOrder> {
    private volatile Boolean flag = true;

    private Random random = new Random();

    private static List<VideoOrder> list = new ArrayList<>();
    static {
        list.add(new VideoOrder("","java",10,0,null));
        list.add(new VideoOrder("","spring boot",15,0,null));
        list.add(new VideoOrder("","springc loud",20,0,null));
        list.add(new VideoOrder("","flink",45,0,null));
        list.add(new VideoOrder("","面试专题第一季",50,0,null));
        list.add(new VideoOrder("","项目大课",1,0,null));
        list.add(new VideoOrder("","kafka",300,0,null));
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
            String id = UUID.randomUUID().toString().substring(30);
            int userId = random.nextInt(10);
            int videoNum = random.nextInt(list.size());
            VideoOrder videoOrder = list.get(videoNum);
            videoOrder.setUserId(userId);
            videoOrder.setCreateTime(new Date());
            videoOrder.setTradeNo(id);
            System.out.println("产生:"+videoOrder.getTitle()+"，价格:"+videoOrder.getMoney()+", 时间:"+ TimeUtil.format(videoOrder.getCreateTime()));
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
