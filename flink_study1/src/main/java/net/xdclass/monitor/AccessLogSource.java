package net.xdclass.monitor;

import net.xdclass.util.TimeUtil;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

/**
 *
 * 
 **/

public class AccessLogSource extends RichParallelSourceFunction<AccessLogDO> {
    private volatile Boolean flag = true;

    private Random random = new Random();

    //接口
    private static List<AccessLogDO> urlList = new ArrayList<>();
    static {
        urlList.add(new AccessLogDO("首页","/pub/api/v1/web/index_card","GET",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("个人信息","/pub/api/v1/web/user_info","GET",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("分类列表","/pub/api/v1/web/all_category","GET",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("分页视频","/pub/api/v1/web/page_video","GET",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("收藏","/user/api/v1/favorite/save","POST",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("下单","/user/api/v1/product/order/save","POST",200,"",new Date(),"",""));
        urlList.add(new AccessLogDO("异常url","","POST",200,"",new Date(),"",""));
    }

    //状态码
    private static List<Integer> codeList = new ArrayList<>();
    static {
        codeList.add(200);
        codeList.add(200);
        codeList.add(200);
        codeList.add(502);
        codeList.add(403);
    }

    @Override
    public void run(SourceContext<AccessLogDO> ctx) throws Exception {
        while (flag){
            Thread.sleep(1000);
            int userId = random.nextInt(50);
            int httpCodeNum = random.nextInt(codeList.size());
            int accessLogNum = random.nextInt(urlList.size());
            AccessLogDO accessLogDO = urlList.get(accessLogNum);
            accessLogDO.setHttpCode(codeList.get(httpCodeNum));
            accessLogDO.setUserId(userId+"");

            long timestamp = System.currentTimeMillis() - random.nextInt(5000);

            accessLogDO.setCreateTime(new Date(timestamp));
            System.out.println("产生:"+accessLogDO.getTitle()+"，状态码:"+accessLogDO.getHttpCode()+", 时间:"+ TimeUtil.format(accessLogDO.getCreateTime()));
            ctx.collect(accessLogDO);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
