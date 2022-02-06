package net.xdclass.sink;

import net.xdclass.model.VideoOrder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 *
 *
 **/
public class MysqlSink extends RichSinkFunction<VideoOrder> {
    private Connection conn;

    private PreparedStatement ps;

    /**
     * 初始化连接
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("open=======");
        conn = DriverManager.getConnection("jdbc:mysql://192.168.11.12:3306/xd_order?useUnicode=true&characterEncoding=utf8&allowMultiQueries=true&serverTimezone=Asia/Shanghai", "root", "123456");

        String sql = "INSERT INTO `video_order` (`user_id`, `money`, `title`, `trade_no`, `create_time`) VALUES(?,?,?,?,?);";
        ps = conn.prepareStatement(sql);
    }

    /**
     * 关闭链接
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        System.out.println("close=======");
        if(conn != null){
            conn.close();
        }
        if(ps != null){
            ps.close();;
        }
    }

    /**
     * 执行对应的sql
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(VideoOrder value, Context context) throws Exception {
        ps.setInt(1,value.getUserId());
        ps.setInt(2,value.getMoney());
        ps.setString(3,value.getTitle());
        ps.setString(4,value.getTradeNo());
        ps.setDate(5,new Date(value.getCreateTime().getTime()));

        ps.executeUpdate();
    }


}
