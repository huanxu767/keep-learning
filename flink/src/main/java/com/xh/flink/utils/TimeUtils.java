package com.xh.flink.utils;

import java.util.Calendar;

public class TimeUtils {

    public static long getDateStart(int i){
        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));
        calendar.add(Calendar.DAY_OF_YEAR, i);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        return calendar.getTimeInMillis();
    }

    public static long getTodayStart(){
      return getDateStart(0);
    }

    public static void main(String[] args) {
        System.out.println(getDateStart(-1));
        }
}
