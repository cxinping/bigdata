package com.dxtd.flink.model;

import com.dxtd.flink.util.TimeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder2 {
    private String tradeNo;
    private String title;
    private int money;
    private int userId;
    private Date createTime;

    @Override
    public String toString() {
        return "VideoOrder2{" +
                "tradeNo='" + tradeNo + '\'' +
                ", title='" + title + '\'' +
                ", money=" + money +
                ", userId=" + userId +
                ", createTime=" + TimeUtil.format(createTime) +
                '}';
    }


}
