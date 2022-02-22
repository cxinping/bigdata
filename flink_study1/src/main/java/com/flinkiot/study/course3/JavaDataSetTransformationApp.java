package com.flinkiot.study.course3;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Author: Michael PK
 */
public class JavaDataSetTransformationApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //mapFunction(env);

//        filterFunction(env);
//        mapPartitionFunction(env);

//        firstFunction(env);
        //flatMapFunction(env);

        distinctFunction(env);
//        joinFunction(env);
//        outerJoinFunction(env);

       // crossFunction(env);
    }

    public static void crossFunction(ExecutionEnvironment env) throws Exception {
        List<String> info1 = new ArrayList<String>();
        info1.add("曼联");
        info1.add("曼城");

        List<String> info2 = new ArrayList<String>();
        info2.add("3");
        info2.add("1");
        info2.add("0");

        DataSource<String> data1 = env.fromCollection(info1);
        DataSource<String> data2 = env.fromCollection(info2);

        data1.cross(data2).print();
    }

    public static void outerJoinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

//        data1.leftOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(second == null) {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
//
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();

//        data1.rightOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer,String, String>>() {
//            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//                if(first == null) {
//                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
//                } else {
//                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
//                }
//            }
//        }).print();


        data1.fullOuterJoin(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                if (first == null) {
                    return new Tuple3<Integer, String, String>(second.f0, "-", second.f1);
                } else if (second == null) {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, "-");
                } else {
                    return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
                }
            }
        }).print();

    }

    public static void joinFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info1 = new ArrayList<Tuple2<Integer, String>>();
        info1.add(new Tuple2(1, "PK哥"));
        info1.add(new Tuple2(2, "J哥"));
        info1.add(new Tuple2(3, "小队长"));
        info1.add(new Tuple2(4, "猪头呼"));


        List<Tuple2<Integer, String>> info2 = new ArrayList<Tuple2<Integer, String>>();
        info2.add(new Tuple2(1, "北京"));
        info2.add(new Tuple2(2, "上海"));
        info2.add(new Tuple2(3, "成都"));
        info2.add(new Tuple2(5, "杭州"));

        DataSource<Tuple2<Integer, String>> data1 = env.fromCollection(info1);
        DataSource<Tuple2<Integer, String>> data2 = env.fromCollection(info2);

        data1.join(data2).where(0).equalTo(0).with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
            public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                return new Tuple3<Integer, String, String>(first.f0, first.f1, second.f1);
            }
        }).print();
    }

    public static void distinctFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).distinct().print();
    }

    public static void flatMapFunction(ExecutionEnvironment env) throws Exception {
        List<String> info = new ArrayList<String>();
        info.add("hadoop,spark");
        info.add("hadoop,flink");
        info.add("flink,flink");

        DataSource<String> data = env.fromCollection(info);

        data.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String input, Collector<String> collector) throws Exception {
                String[] splits = input.split(",");
                for (String split : splits) {
                    collector.collect(split);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).groupBy(0).sum(1).print();

    }

    public static void firstFunction(ExecutionEnvironment env) throws Exception {
        List<Tuple2<Integer, String>> info = new ArrayList<Tuple2<Integer, String>>();
        info.add(new Tuple2(1, "Hadoop"));
        info.add(new Tuple2(1, "Spark"));
        info.add(new Tuple2(1, "Flink"));
        info.add(new Tuple2(2, "Java"));
        info.add(new Tuple2(2, "Spring Boot"));
        info.add(new Tuple2(3, "Linux"));
        info.add(new Tuple2(4, "VUE"));

        DataSource<Tuple2<Integer, String>> data = env.fromCollection(info);

        data.first(3).print();
        System.out.println("~~~~~~~~~~");

        data.groupBy(0).first(2).print();
        System.out.println("~~~~~~~~~~");

        data.groupBy(0).sortGroup(1, Order.DESCENDING).first(2).print();

    }


    public static void mapPartitionFunction(ExecutionEnvironment env) throws Exception {
        List<String> list = new ArrayList<String>();
        for (int i = 1; i <= 100; i++) {
            list.add("student: " + i);
        }
        DataSource<String> data = env.fromCollection(list).setParallelism(6);

//        data.map(new MapFunction<String, String>() {
//            public String map(String input) throws Exception {
//                String connection = DBUtils.getConection();
//                System.out.println("connection = [" + connection + "]");
//                DBUtils.returnConnection(connection);
//                return input;
//            }
//        }).print();

//        data.mapPartition(new MapPartitionFunction<String, String>() {
//            public void mapPartition(Iterable<String> inputs, Collector<String> collector) throws Exception {
//                String connection = DBUtils.getConection();
//                System.out.println("connection = [" + connection + "]");
//                DBUtils.returnConnection(connection);
//            }
//        }).print();

    }

    public static void filterFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).filter(new FilterFunction<Integer>() {
            public boolean filter(Integer input) throws Exception {
                return input > 5;
            }
        }).print();
    }

    public static void mapFunction(ExecutionEnvironment env) throws Exception {
        List<Integer> list = new ArrayList<Integer>();
        for (int i = 1; i <= 10; i++) {
            list.add(i);
        }
        DataSource<Integer> data = env.fromCollection(list);

        data.map(new MapFunction<Integer, Integer>() {
            public Integer map(Integer input) throws Exception {
                return input + 1;
            }
        }).print();
    }
}
