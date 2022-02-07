package net.xdclass.project;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;
import java.util.Scanner;
import java.util.stream.Stream;

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

    public static void main(String[] args) throws IOException {
        String fileName = "data/user_log1.csv";
        File file = new File(fileName);

        // 读取文件内容到Stream流中，按行读取
        Stream<String> lines = Files.lines(Paths.get(fileName));

        // 按照顺序进行数据处理
        lines.forEachOrdered(ele -> {
            String[] line =ele.split(",");
            String user_id = line[0];

            if( !user_id.equals("user_id")) { //买家id在每行日志代码的第0个元素,去除第一行表头
                System.out.println(ele);
            }

        });

    }

}
