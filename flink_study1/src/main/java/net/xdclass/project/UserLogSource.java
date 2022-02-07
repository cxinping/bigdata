package net.xdclass.project;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.util.Random;

public class UserLogSource implements SourceFunction<UserLog>{
    boolean running = true;

    @Override
    public void run(SourceContext<UserLog> ctx) throws Exception {
        
        while (running) {
            for (int i = 0; i < 10 ; i++) {


                //ctx.collect(access);
            }

            Thread.sleep(5 * 1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) {
        File file = new File("");



    }

}
