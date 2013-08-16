package de.aonnet.ds.esc

import org.elasticsearch.common.joda.time.Period
import spock.lang.Specification

import java.text.SimpleDateFormat

/**
 * User: westphal
 * Date: 13.08.13
 * Time: 08:45
 */
class TimeSpanCalculatorSpec extends Specification {

    SimpleDateFormat testDateFormat = new SimpleDateFormat('dd.MM.yyyy HH:mm:SS')

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator then set all fields'() {

        setup:
        Date from = testDateFormat.parse('01.02.2013 00:00:00')
        Date to = testDateFormat.parse('02.02.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == from
        calculator.to.toDate() == to
        calculator.months == 0
        calculator.days == 1
        calculator.fullMinutesAtFirstDay
        !calculator.fullMinutesAtLastDay
        !calculator.fullDaysAtFirstMonth
        !calculator.fullDaysAtLastMonth
    }

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator then set period months and days'() {

        setup:
        Date from = testDateFormat.parse('01.02.2013 00:00:00')
        Date to = testDateFormat.parse('01.03.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == from
        calculator.to.toDate() == to
        calculator.months == 1
        calculator.days == 28
        calculator.fullMinutesAtFirstDay
        !calculator.fullMinutesAtLastDay
        calculator.fullDaysAtFirstMonth
        !calculator.fullDaysAtLastMonth
    }

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator then set month over years'() {

        setup:
        Date from = testDateFormat.parse('01.02.2013 00:00:00')
        Date to = testDateFormat.parse('31.01.2014 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == from
        calculator.to.toDate() == to
        calculator.months == 11
        calculator.days == 364
        calculator.fullMinutesAtFirstDay
        !calculator.fullMinutesAtLastDay
        calculator.fullDaysAtFirstMonth
        !calculator.fullDaysAtLastMonth
    }

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator then set month over years with full month'() {

        setup:
        Date from = testDateFormat.parse('01.02.2013 00:00:01')
        Date to = testDateFormat.parse('31.01.2014 23:59:02')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == testDateFormat.parse('01.02.2013 00:00:00')
        calculator.to.toDate() == testDateFormat.parse('31.01.2014 23:59:00')
        calculator.months == 11
        calculator.days == 364
        calculator.fullMinutesAtFirstDay
        calculator.fullMinutesAtLastDay
        calculator.fullDaysAtFirstMonth
        calculator.fullDaysAtLastMonth
    }

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator with minutes then set full minutes'() {

        setup:
        Date from = testDateFormat.parse('01.02.2013 00:01:00')
        Date to = testDateFormat.parse('31.01.2014 23:59:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == from
        calculator.to.toDate() == to
        calculator.months == 11
        calculator.days == 364
        !calculator.fullMinutesAtFirstDay
        calculator.fullMinutesAtLastDay
        !calculator.fullDaysAtFirstMonth
        calculator.fullDaysAtLastMonth
    }

    @SuppressWarnings("GroovyAccessibility")
    def 'when create TimeSpanCalculator then set days over years'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('01.03.2015 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.from.toDate() == from
        calculator.to.toDate() == to
        calculator.months == 17
        calculator.days == 516
    }

    def 'when no timespan then all timespans are empty'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('01.10.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.empty
        calculator.minuteTimeSpans.empty
    }

    def 'when only seconds then minutes timespan empty'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('01.10.2013 00:00:31')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.empty
        calculator.minuteTimeSpans.empty
    }

    def 'when only one minute then minutes timespan are filled'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('01.10.2013 00:01:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.empty
        calculator.minuteTimeSpans.size() == 1
        calculator.minuteTimeSpans[0].from == from
        calculator.minuteTimeSpans[0].to == to
    }

    def 'when only one day then day timespan are filled and minutes are empty'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('02.10.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.size() == 1
        calculator.dayTimeSpans[0].from == from
        calculator.dayTimeSpans[0].to == testDateFormat.parse('01.10.2013 23:59:00')
        calculator.minuteTimeSpans.empty
    }

    def 'when not only full days then day and minutes timespan are filled'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('14.10.2013 00:01:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.size() == 1
        calculator.dayTimeSpans[0].from == from
        calculator.dayTimeSpans[0].to == testDateFormat.parse('13.10.2013 23:59:00')
        calculator.minuteTimeSpans.size() == 1
        calculator.minuteTimeSpans[0].from == testDateFormat.parse('14.10.2013 00:00:00')
        calculator.minuteTimeSpans[0].to == testDateFormat.parse('14.10.2013 00:01:00')
    }

    def 'when only full days but 00:00:00 then day timespan are filled with last day'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:00:00')
        Date to = testDateFormat.parse('14.10.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.dayTimeSpans.size() == 1
        calculator.dayTimeSpans[0].from == from
        calculator.dayTimeSpans[0].to == testDateFormat.parse('13.10.2013 23:59:00')
        calculator.minuteTimeSpans.empty
    }

    def 'when only minutes before full day then day timespan are filled and one minutes timestamp'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:23:00')
        Date to = testDateFormat.parse('14.10.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan == null
        calculator.minuteTimeSpans.size() == 1
        calculator.minuteTimeSpans[0].from == from
        calculator.minuteTimeSpans[0].to == testDateFormat.parse('01.10.2013 23:59:00')
        calculator.dayTimeSpans.size() == 1
        calculator.dayTimeSpans[0].from == testDateFormat.parse('02.10.2013 00:00:00')
        calculator.dayTimeSpans[0].to == testDateFormat.parse('13.10.2013 23:59:00')
    }

    def 'when full month then create month time span'() {

        setup:
        Date from = testDateFormat.parse('01.10.2013 00:23:00')
        Date to = testDateFormat.parse('14.12.2013 00:00:00')

        when:
        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        then:
        calculator.monthTimeSpan.from == testDateFormat.parse('01.11.2013 00:00:00')
        calculator.monthTimeSpan.to == testDateFormat.parse('30.11.2013 23:59:00')
        calculator.dayTimeSpans.size() == 2
        calculator.dayTimeSpans[0].from == testDateFormat.parse('02.10.2013 00:00:00')
        calculator.dayTimeSpans[0].to == testDateFormat.parse('31.10.2013 23:59:00')
        calculator.dayTimeSpans[1].from == testDateFormat.parse('01.12.2013 00:00:00')
        calculator.dayTimeSpans[1].to == testDateFormat.parse('13.12.2013 23:59:00')
        calculator.minuteTimeSpans.size() == 1
        calculator.minuteTimeSpans[0].from == from
        calculator.minuteTimeSpans[0].to == testDateFormat.parse('01.10.2013 23:59:00')
    }
}
