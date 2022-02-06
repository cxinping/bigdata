package net.xdclass.monitor;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 *
 *
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
