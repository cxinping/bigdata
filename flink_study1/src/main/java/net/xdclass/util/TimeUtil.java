package net.xdclass.util;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 *
 *
 **/

public class TimeUtil {


    /**
     *  time 转 字符串
     * @param time
     * @return
     */
    public static String format(Date time){
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        String timeStr = formatter.format(time.toInstant().atZone(zoneId));
        return timeStr;
    }

    /**
     * timestamp 转 字符串
     *
     * @param timestamp
     * @return
     */
    public static String format(long timestamp) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        ZoneId zoneId = ZoneId.systemDefault();
        String timeStr = formatter.format(new Date(timestamp).toInstant().atZone(zoneId));
        return timeStr;
    }


    /**
     * 字符串 转 date
     *
     * @param time
     * @return
     */
    public static Date strToDate(String time) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        LocalDateTime localDateTime = LocalDateTime.parse(time, formatter);
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());

    }
}
