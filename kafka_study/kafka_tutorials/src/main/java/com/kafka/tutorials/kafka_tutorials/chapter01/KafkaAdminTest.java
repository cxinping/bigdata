package com.kafka.tutorials.kafka_tutorials.chapter01;

import org.apache.kafka.clients.admin.*;

import java.util.*;
import java.util.concurrent.ExecutionException;


public class KafkaAdminTest {

    private static final String TOPIC_NAME = "test";

    /**
     * 设置admin 客户端
     * @return
     */
    public static AdminClient initAdminClient(){
        Properties properties = new Properties();
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.11.12:9092");

        AdminClient adminClient = AdminClient.create(properties);
        return adminClient;
    }

    public static void createTopicTest(){
        /**
         * 创建 topic
         *
         * */
        AdminClient adminClient = initAdminClient();

        //指定分区数量，副本数量。 两个分区，一个副本
        NewTopic newTopic = new NewTopic(TOPIC_NAME,2,(short) 1);

        CreateTopicsResult createTopicsResult = adminClient.createTopics(Arrays.asList(newTopic));
        try {
            //future等待创建，成功则不会有任何报错
            createTopicsResult.all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 列举topic列表
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void listTopicTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();

        //是否查看内部的topic，可以不用
        ListTopicsOptions options = new ListTopicsOptions();
        //options.listInternal(true);

        ListTopicsResult listTopicsResult = adminClient.listTopics(options);

        Set<String> topics = listTopicsResult.names().get();
        for(String name : topics){
            System.err.println(name);
        }

    }

    /**
     * 删除topic
     */
    public static void delTopicTest() throws ExecutionException, InterruptedException {
        AdminClient adminClient = initAdminClient();

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Arrays.asList("test2","test3","xpclass-test"));

        deleteTopicsResult.all().get();
    }

    /**
     * 查看某个topic详情
     */
    public static void detailTopicTest() throws ExecutionException, InterruptedException {

        AdminClient adminClient = initAdminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Arrays.asList(TOPIC_NAME));

        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();

        Set<Map.Entry<String, TopicDescription>> entries = stringTopicDescriptionMap.entrySet();

        entries.stream().forEach((entry)-> System.out.println("name ："+entry.getKey()+" , desc: "+ entry.getValue()));
    }

    /**
     * 增加topic分区数量
     * @throws ExecutionException
     * @throws InterruptedException
     */
    public static void incrPartitionTopicTest() throws ExecutionException, InterruptedException {
        Map<String,NewPartitions> infoMap = new HashMap<>(1);


        AdminClient adminClient = initAdminClient();
        // 增加5个分区
        NewPartitions newPartitions = NewPartitions.increaseTo(5);

        infoMap.put(TOPIC_NAME,newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(infoMap);

        createPartitionsResult.all().get();

    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        //createTopicTest();

        /**
         * /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka1:9092
         * 查看已经创建的topic
         * */

        //listTopicTest();

        //delTopicTest();

       // detailTopicTest();

        incrPartitionTopicTest();

    }



}
