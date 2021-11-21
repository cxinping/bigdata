package com.kafka.tutorials.kafka_tutorials.chapter01;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class KafkaAdminTest {

    private static final String TOPIC_NAME = "test3";

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

        //指定分区数量，副本数量
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

    public static void main(String[] args) {
        createTopicTest();

        /**
         * /usr/local/kafka/bin/kafka-topics.sh --list --bootstrap-server kafka1:9092
         *
         * */

    }



}
