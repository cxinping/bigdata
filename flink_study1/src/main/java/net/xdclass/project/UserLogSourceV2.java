package net.xdclass.project;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

public class UserLogSourceV2 implements ParallelSourceFunction<UserLog> {
    boolean running = true;

    @Override
    public void run(SourceContext<UserLog> ctx) throws IOException {
        String fileName = "data/user_log1.csv";
        File file = new File(fileName);
        // 读取文件内容到Stream流中，按行读取
        Stream<String> lines = Files.lines(Paths.get(fileName));

        while (running) {
            // 按照顺序进行数据处理
            lines.forEachOrdered(ele -> {
                String[] line =ele.split(",");
                String user_id = line[0];

                if( !user_id.equals("user_id")) { //买家id在每行日志代码的第0个元素,去除第一行表头
                    //System.out.println(ele);
                    UserLog userLog = new UserLog();
                    userLog.setUser_id(line[0]);
                    userLog.setItem_id(line[1]);
                    userLog.setCat_id(line[2]);
                    userLog.setMonth(line[5]);
                    userLog.setDay(line[6]);
                    userLog.setGender(line[9]);
                    userLog.setProvince(line[10]);

                    ctx.collect(userLog);

                    try {
                        Thread.sleep(10 * 1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            });
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws Exception {
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

                try {
                    Thread.sleep(1 * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

        });

    }

}
