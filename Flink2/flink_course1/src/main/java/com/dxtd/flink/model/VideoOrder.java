package  com.dxtd.flink.model;

import com.dxtd.flink.util.TimeUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import java.util.Date;


//订单实体类

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VideoOrder {

    private String tradeNo;

    private String title;

    private int money;

}