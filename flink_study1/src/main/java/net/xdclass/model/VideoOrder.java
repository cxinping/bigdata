package net.xdclass.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import net.xdclass.util.TimeUtil;

import java.util.Date;

/**
 *
 *
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

    public static void main(String[] args) {
        VideoOrder videoOrder = new VideoOrder();
        videoOrder.setUserId(1001);
        videoOrder.setCreateTime(new Date());
        System.out.println(videoOrder);

    }

}