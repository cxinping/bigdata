package com.dxtd.flink.chapter02.source;

import com.dxtd.flink.chapter02.transformation.Access;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Random;

public class AccessSource implements SourceFunction<Access> {

    boolean running = true;

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {

        String[] domains = {"d.com", "a.com","b.com"};

        Random random = new Random();

        while (running) {
            for (int i = 0; i < 5 ; i++) {
                Access access = new Access();
                access.setTime(1234567L);
                access.setDomain(domains[random.nextInt(domains.length)]);
                access.setTraffic(random.nextDouble() + 1000);

                ctx.collect(access);
            }

            Thread.sleep(10*1000);
            System.out.println("");
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
