package com.dxtd.app;

import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TupleApp {

    public static void main(String [] args){
        //tuple测试
        //tupleTest();
        mapTest();
        //flatMapTest();

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
