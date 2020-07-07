package com.xh.flink.utils;

import java.util.Calendar;

public class TimeUtils {

    public static long getTodayStart(){
        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }
}
