package com.dxtd.flink.app;

import com.dxtd.flink.model.VideoOrder;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class JdkStreamApp {

    public static void main(String [] args){
        //总价 35
        List<VideoOrder> videoOrders1 = Arrays.asList(
                new VideoOrder("20190242812", "springboot教程", 3),
                new VideoOrder("20194350812", "微服务SpringCloud", 5),
                new VideoOrder("20190814232", "Redis教程", 9),
                new VideoOrder("20190523812", "⽹⻚开发教程", 9),
                new VideoOrder("201932324", "百万并发实战Netty", 9));
        //总价 54
        List<VideoOrder> videoOrders2 = Arrays.asList(
                new VideoOrder("2019024285312", "springboot教程", 3),
                new VideoOrder("2019081453232", "Redis教程", 9),
                new VideoOrder("20190522338312", "⽹⻚开发教程", 9),
                new VideoOrder("2019435230812", "Jmeter压⼒测试", 5),
                new VideoOrder("2019323542411", "Git+Jenkins持续集成", 7),
                new VideoOrder("2019323542424", "Idea全套教程", 21));

        //平均价格
        double videoOrder1Avg1 = videoOrders1.stream().
                collect(Collectors.averagingInt(VideoOrder::getMoney))
                .doubleValue();

        double videoOrder1Avg2 = videoOrders2.stream().
                collect(Collectors.averagingInt(VideoOrder::getMoney))
                .doubleValue();

        System.out.println("videoOrder1Avg1="+videoOrder1Avg1);
        System.out.println("videoOrder1Avg2="+videoOrder1Avg2);

        //订单总价
        int total1 = videoOrders1.stream().collect(Collectors.summingInt(VideoOrder::getMoney)).intValue();
        int total2 = videoOrders2.stream().collect(Collectors.summingInt(VideoOrder::getMoney)).intValue();

        System.out.println("total1="+total1);
        System.out.println("total2="+total2);
    }

}
