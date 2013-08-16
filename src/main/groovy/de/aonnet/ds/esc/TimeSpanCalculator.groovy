package de.aonnet.ds.esc

import groovy.transform.ToString
import org.elasticsearch.common.joda.time.DateTime
import org.elasticsearch.common.joda.time.DateTimeZone
import org.elasticsearch.common.joda.time.Interval
import org.elasticsearch.common.joda.time.PeriodType
import org.elasticsearch.common.joda.time.format.DateTimeFormat
import org.elasticsearch.common.joda.time.format.DateTimeFormatter

/**
 * User: westphal
 * Date: 13.08.13
 * Time: 08:45
 */
@ToString(includeNames = true, includePackage = false, excludes = 'fromCalendar, toCalendar')
class TimeSpanCalculator {

    private static final logEnabled = false

    private DateTimeFormatter fmt = DateTimeFormat.forPattern 'dd.MM.yyyy HH:mm'

    private final DateTime from
    private final DateTime to

    private final DateTime firstFullDay
    private final DateTime lastFullDay

    private final DateTime firstMinuteAtFirstDay
    private final DateTime lastMinuteAtFirstDay
    private final DateTime firstMinuteAtLastDay
    private final DateTime lastMinuteAtLastDay
    private final DateTime fromMinuteAtLastDay
    private final DateTime toMinuteAtFirstDay

    private final DateTime firstDayAtFirstMonth
    private final DateTime lastDayAtFirstMonth
    private final DateTime firstDayAtLastMonth
    private final DateTime lastDayAtLastMonth

    private final boolean fullMinutesAtFirstDay
    private final boolean fullMinutesAtLastDay
    private final boolean emptyMinutesAtLastDay
    private final boolean fullDaysAtFirstMonth
    private final boolean fullDaysAtLastMonth

    private final long months
    private final long days
    private final long minutes

    TimeSpanCalculator(Date fromDate, Date toDate) {

        from = new DateTime(fromDate, DateTimeZone.forID('UTC')).withSecondOfMinute(0).withMillisOfSecond(0)
        to = new DateTime(toDate, DateTimeZone.forID('UTC')).withSecondOfMinute(0).withMillisOfSecond(0)

        Interval intervalAll = new Interval(from, to);

        firstMinuteAtFirstDay = from.withHourOfDay(0).withMinuteOfHour(0)
        firstMinuteAtLastDay = to.withHourOfDay(0).withMinuteOfHour(0)
        lastMinuteAtFirstDay = from.withHourOfDay(23).withMinuteOfHour(59)
        lastMinuteAtLastDay = to.withHourOfDay(23).withMinuteOfHour(59)

        toMinuteAtFirstDay = to.isBefore(lastMinuteAtFirstDay) ? to : lastMinuteAtFirstDay
        fromMinuteAtLastDay = from.isAfter(firstMinuteAtLastDay) ? from : firstMinuteAtLastDay

        log "from:                  ${from.toString(fmt)}"
        log "to:                    ${to.toString(fmt)}"

        Interval intervalMinutesAtFirstDay = new Interval(from, toMinuteAtFirstDay);
        Interval intervalMinutesAtLastDay = new Interval(fromMinuteAtLastDay, to);
        Interval intervalAllMinutesAtFirstDay = new Interval(firstMinuteAtFirstDay, lastMinuteAtFirstDay);
        Interval intervalAllMinutesAtLastDay = new Interval(firstMinuteAtLastDay, lastMinuteAtLastDay);

        fullMinutesAtFirstDay = intervalMinutesAtFirstDay.toPeriod(PeriodType.minutes()).minutes == intervalAllMinutesAtFirstDay.toPeriod(PeriodType.minutes()).minutes
        fullMinutesAtLastDay = intervalMinutesAtLastDay.toPeriod(PeriodType.minutes()).minutes == intervalAllMinutesAtLastDay.toPeriod(PeriodType.minutes()).minutes
        emptyMinutesAtLastDay = intervalMinutesAtLastDay.toPeriod(PeriodType.minutes()).minutes == 0

        log "fullMinutesAtFirstDay: ${fullMinutesAtFirstDay}"
        log "fullMinutesAtLastDay:  ${fullMinutesAtLastDay}"
        log "emptyMinutesAtLastDay: ${emptyMinutesAtLastDay}"

        if (fullMinutesAtFirstDay) {
            firstFullDay = from
        } else {
            firstFullDay = lastMinuteAtFirstDay.plusMinutes(1)
        }


        if (fullMinutesAtLastDay) {
            lastFullDay = to
        } else {
            lastFullDay = firstMinuteAtLastDay.minusMinutes(1)
        }

        log "firstFullDay:          ${firstFullDay.toString(fmt)}"
        log "lastFullDay:           ${lastFullDay.toString(fmt)}"

        firstDayAtFirstMonth = from.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
        firstDayAtLastMonth = to.withDayOfMonth(1).withHourOfDay(0).withMinuteOfHour(0)
        lastDayAtFirstMonth = from.withDayOfMonth(from.dayOfMonth().maximumValue).withHourOfDay(23).withMinuteOfHour(59)
        lastDayAtLastMonth = to.withDayOfMonth(to.dayOfMonth().maximumValue).withHourOfDay(23).withMinuteOfHour(59)

        log "firstDayAtFirstMonth:  ${firstDayAtFirstMonth.toString(fmt)}"
        log "firstDayAtLastMonth:   ${firstDayAtLastMonth.toString(fmt)}"
        log "lastDayAtFirstMonth:   ${lastDayAtFirstMonth.toString(fmt)}"
        log "lastDayAtLastMonth:    ${lastDayAtLastMonth.toString(fmt)}"

        fullDaysAtFirstMonth = firstFullDay.isEqual(firstDayAtFirstMonth) &&
                (lastFullDay.isEqual(lastDayAtFirstMonth) || lastFullDay.isAfter(lastDayAtFirstMonth))
        fullDaysAtLastMonth = lastFullDay.isEqual(lastDayAtLastMonth) &&
                (firstFullDay.isEqual(firstDayAtLastMonth) || firstFullDay.isBefore(firstDayAtLastMonth))

        log "fullDaysAtFirstMonth:  ${fullDaysAtFirstMonth}"
        log "fullDaysAtLastMonth:   ${fullDaysAtLastMonth}"

        months = intervalAll.toPeriod(PeriodType.months()).months
        days = intervalAll.toPeriod(PeriodType.days()).days
        minutes = intervalAll.toPeriod(PeriodType.minutes()).minutes
    }

    TimeSpan getMonthTimeSpan() {

        if (months <= 1) {

            // no month
            return null
        }

        DateTime fromDay
        DateTime toDay

        if (fullDaysAtFirstMonth) {
            fromDay = firstFullDay
        } else {
            fromDay = firstDayAtFirstMonth.plusMonths(1)
        }
        if (fullDaysAtLastMonth) {
            toDay = lastFullDay
        } else {
            toDay = lastDayAtLastMonth.minusMonths(1)
        }

        return new TimeSpan(from: fromDay.toDate(), to: toDay.toDate(), timeSpanType: TimeSpanType.DAY)

    }

    List<TimeSpan> getDayTimeSpans() {

        if (days < 1) {

            // no days
            return []
        }

        if (months <= 1) {

            return [new TimeSpan(from: firstFullDay.toDate(), to: lastFullDay.toDate(), timeSpanType: TimeSpanType.DAY)]
        }

        List<TimeSpan> timeSpans = []

        // filter for days before first full month
        if (!fullDaysAtFirstMonth) {
            timeSpans << new TimeSpan(from: firstFullDay.toDate(), to: lastDayAtFirstMonth.toDate(), timeSpanType: TimeSpanType.DAY)
        }
        if (!fullDaysAtLastMonth) {
            timeSpans << new TimeSpan(from: firstDayAtLastMonth.toDate(), to: lastFullDay.toDate(), timeSpanType: TimeSpanType.DAY)
        }

        return timeSpans
    }

    List<TimeSpan> getMinuteTimeSpans() {

        if (minutes < 1) {

            // no minutes
            return []
        } else if (minutes % 1440 == 0) {

            // one days no minutes
            return []
        } else if (minutes < 1440) {

            // only minutes no days
            return [new TimeSpan(from: from.toDate(), to: to.toDate(), timeSpanType: TimeSpanType.MINUTE)]
        }

        // more than one day
        List<TimeSpan> timeSpans = []

        // filter for minutes before first full day
        if (!fullMinutesAtFirstDay) {
            timeSpans << new TimeSpan(from: from.toDate(), to: lastMinuteAtFirstDay.toDate(), timeSpanType: TimeSpanType.MINUTE)
        }
        if (!fullMinutesAtLastDay && !emptyMinutesAtLastDay) {
            timeSpans << new TimeSpan(from: firstMinuteAtLastDay.toDate(), to: to.toDate(), timeSpanType: TimeSpanType.MINUTE)
        }

        return timeSpans
    }

    List<TimeSpan> getTimeSpans() {
        List<TimeSpan> timeSpans = []

        TimeSpan monthTimeSpan = monthTimeSpan
        if (monthTimeSpan) {
            timeSpans << monthTimeSpan
        }

        timeSpans.addAll dayTimeSpans
        timeSpans.addAll minuteTimeSpans

        return timeSpans
    }

    private static void log(String msg) {
        if(logEnabled) {
            println msg
        }
    }
}
