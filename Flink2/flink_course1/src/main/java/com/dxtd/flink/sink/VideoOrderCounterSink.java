package com.dxtd.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 *
 *
 **/

public class VideoOrderCounterSink implements RedisMapper<Tuple2<String, Integer>> {


    /***
     * 选择需要用到的命令，和key名称
     * @return
     */
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "VIDEO_ORDER_COUNTER");
    }

    /**
     * 获取对应的key或者filed
     *
     * @param data
     * @return
     */
    @Override
    public String getKeyFromData(Tuple2<String, Integer> data) {

        System.out.println("getKeyFromData=" + data.f0);
        return data.f0;
    }

    /**
     * 获取对应的值
     *
     * @param data
     * @return
     */
    @Override
    public String getValueFromData(Tuple2<String, Integer> data) {
        System.out.println("getValueFromData=" + data.f1.toString());
        return data.f1.toString();
    }
}
