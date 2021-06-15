package com.xh.flink.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

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

    public static String tsToString(long time){
        Date d = new Date(time);
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sf.format(d);
    }

    public static void main(String[] args) {

//        1623168351250 00:16:04
        System.out.println(tsToString(new Long("1623286803887")));

        System.out.println(tsToString(getDateStart(0)));
        }
}
