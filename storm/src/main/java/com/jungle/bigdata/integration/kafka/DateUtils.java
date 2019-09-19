package com.jungle.bigdata.integration.kafka;


import org.apache.commons.lang3.time.FastDateFormat;

/**
 * 时间解析工具类
 */
public class DateUtils {

    private DateUtils(){}

    private static DateUtils instance;

    /**
     * 单例模式
     * @return
     */
    public static DateUtils getInstance(){
        if (instance == null) {
            instance = new DateUtils();
        }

        return instance;
    }

    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

    /**
     * 转化为时间戳
     * @param time
     * @return
     * @throws Exception
     */
    public long getTime(String time) throws Exception {
        return format.parse(time.substring(1, time.length()-1)).getTime();
    }

}