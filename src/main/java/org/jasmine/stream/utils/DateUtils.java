package org.jasmine.stream.utils;

import java.time.ZoneId;
import java.util.Calendar;
import java.util.TimeZone;

public class DateUtils {

    public static Calendar parseCalendar(long timestamp) {
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault()));
        calendar.setTimeInMillis(timestamp * 1000);
        return calendar;
    }
}
