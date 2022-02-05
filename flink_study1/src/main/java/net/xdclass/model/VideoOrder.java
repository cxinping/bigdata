package net.xdclass.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.xdclass.util.TimeUtil;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Date;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder {
    private String tradeNo;
    private String title;
    private int money;
    private int userId;
    private Date createTime;

    @Override
    public String toString() {
        return "VideoOrder{" +
                "tradeNo='" + tradeNo + '\'' +
                ", title='" + title + '\'' +
                ", money=" + money +
                ", userId=" + userId +
                ", createTime=" + TimeUtil.format(createTime) +
                '}';
    }
}