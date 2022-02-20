package com.flinkiot.study.course3;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Michael PK
 */
public class JavaDataSetSinkApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        List<Integer> info = new ArrayList<Integer>();
        for(int i=1; i<=10; i++) {
            info.add(i);
        }

        String filePath = "/Users/rocky/IdeaProjects/imooc-workspace/data/04/sink-out-java/";
        DataSource<Integer> data = env.fromCollection(info);
        data.writeAsText(filePath, FileSystem.WriteMode.OVERWRITE);


        env.execute("JavaDataSetSinkApp");
    }
}
