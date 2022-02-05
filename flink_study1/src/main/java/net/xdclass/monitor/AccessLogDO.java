package net.xdclass.monitor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

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
@NoArgsConstructor
@AllArgsConstructor
public class AccessLogDO {
    private String title;

    private String url;

    private String method;

    private Integer httpCode;

    private String body;

    private Date createTime;

    private String userId;

    private String city;
}
