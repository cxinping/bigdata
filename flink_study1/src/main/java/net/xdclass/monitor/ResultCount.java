package net.xdclass.monitor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 小滴课堂,愿景：让技术不再难学
 *
 * @Description
 * @Author 二当家小D
 * @Remark 有问题直接联系我，源码-笔记-技术交流群
 * @Version 1.0
 **/

@AllArgsConstructor
@NoArgsConstructor
@Data
public class ResultCount {
    private String url;

    private Integer httpCode;

    private Long count;

    private String startTime;

    private String endTime;

    private String type;
}
