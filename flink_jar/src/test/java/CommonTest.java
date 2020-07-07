import org.apache.flink.util.TimeUtils;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

public class CommonTest {

    @Test
    public void test(){
        //获取今天凌晨的时间戳

        Calendar calendar = Calendar.getInstance();
//        calendar.setTimeZone(TimeZone.getTimeZone("GMT+8:00"));

//        1594051200000

        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.MILLISECOND, 0);

        long todayBegin = calendar.getTimeInMillis();
        System.out.println("当前时间戳" + todayBegin);

        System.out.println(TimeZone.getDefault());


//        TimeZone.getDefault().getID()
        System.out.println(timestampToStr(todayBegin,"GMT+8"));
    }

    /**
     * 时间戳转字符串
     *
     * @param timestamp 毫秒级时间戳
     * @param zoneId    如 GMT+8或UTC+08:00
     * @return
     */
    public static String timestampToStr(long timestamp, String zoneId) {
        ZoneId timezone = ZoneId.of(zoneId);
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), timezone);
        return localDateTime.toString();
    }

}
