package de.aonnet.ds.esc

import org.apache.commons.lang.NotImplementedException

import java.text.SimpleDateFormat

/**
 * User: westphal
 * Date: 16.08.13
 * Time: 06:44
 */
class DsHelper {

    static SimpleDateFormat getFormatIdMinute() {
        SimpleDateFormat formatIdMinute = new SimpleDateFormat("yyyyMMdd_HHmm")
        formatIdMinute.setTimeZone(TimeZone.getTimeZone('UTC'))
        return formatIdMinute
    }

    static SimpleDateFormat getFormatIdDay() {
        SimpleDateFormat formatIdDay = new SimpleDateFormat("yyyyMMdd")
        formatIdDay.setTimeZone(TimeZone.getTimeZone('UTC'))
        return formatIdDay
    }

    static SimpleDateFormat getFormatIdMonth() {
        SimpleDateFormat formatIdMonth = new SimpleDateFormat("yyyyMM")
        formatIdMonth.setTimeZone(TimeZone.getTimeZone('UTC'))
        return formatIdMonth
    }

    static SimpleDateFormat getFormatForTimeSpanType(TimeSpanType timeSpanType) {
        switch (timeSpanType) {

            case TimeSpanType.MINUTE:
                return formatIdMinute

            case TimeSpanType.DAY:
                return formatIdDay

            case TimeSpanType.MONTH:
                return formatIdMonth
        }

        throw new NotImplementedException("Format for TimeSpanType $timeSpanType not implemented.")
    }
}
