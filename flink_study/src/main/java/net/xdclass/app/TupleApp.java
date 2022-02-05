package net.xdclass.app;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

public class TupleApp {

    public static void main(String [] args){

        //tuple测试
        //tupleTest();
        mapTest();
        flatMapTest();

    }

    private static void tupleTest(){
        Tuple3<Integer,String,Long> tuple3 =  Tuple3.of(1,"xdclass.net",120L);

        System.out.println(tuple3.f0);
        System.out.println(tuple3.f1);
        System.out.println(tuple3.f2);
    }


    private static void mapTest(){
        List<String> list1 = new ArrayList<>();
        list1.add("springboot,springcloud");
        list1.add("redis6,docker");
        list1.add("kafka,rabbitmq");

        List<String> result = list1.stream().map(obj->{

            obj = "小滴课堂"+obj;
            return  obj;
        }).collect(Collectors.toList());

        System.out.println(result);

    }


    private static void flatMapTest(){
        List<String> list1 = new ArrayList<>();
        list1.add("springboot,springcloud");
        list1.add("redis6,docker");
        list1.add("kafka,rabbitmq");


        List<String> result =  list1.stream().flatMap(obj->{
            Stream<String> stream =  Arrays.stream( obj.split(","));
            return stream;
        }).collect(Collectors.toList());

        System.out.println(result);



    }




}
