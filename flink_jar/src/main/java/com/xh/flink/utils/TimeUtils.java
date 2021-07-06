package com.xh.flink.utils;

import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
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

    public static long getDateStart(String i) throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        if(i == null){
            return getDateStart(0);
        }
        Date date = sf.parse(i);
        return date.getTime();
    }

    public static String tsToString(long time){
        Date d = new Date(time);
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sf.format(d);
    }

    public static void main(String[] args) throws ParseException {
//        es
        System.out.println(tsToString(1625476893859l));
//        System.out.println(tsToString(1624981190735l));
//
//        long t = getDateStart("2021-06-16 00:00:00");
//        System.out.println(t);
//        System.out.println(tsToString(t));
//
//        long t1 = getDateStart(null);
//        System.out.println(t1);
//        System.out.println(tsToString(t1));
    }
}
