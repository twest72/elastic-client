package de.aonnet.ds.esc

import groovy.transform.AutoClone
import groovy.transform.ToString

/**
 * User: westphal
 * Date: 16.08.13
 * Time: 06:47
 */
@AutoClone
@ToString(includePackage = false, includeNames = true)
class DsImportData {

    static final String DATE_ID = 'date_id'
    static final String DIMENSION_ID = 'dimension_id'
    static final String DAY_OF_MONTH = 'day_of_month'
    static final String WEEK_OF_YEAR = 'week_of_year'
    static final String HOUR_OF_DAY = 'hour_of_day'
    static final String MINUTE = 'minute'

    String id
    String dateId
    String dimensionId
    String dimension

    Date date
    Integer year
    Integer month
    Integer dayOfMonth
    Integer weekOfYear
    Integer hourOfDay
    Integer minute
    Integer second

    Map<String, ? extends Object> data

    void prepareForMonth() {
        dateId = "${DsHelper.formatIdMonth.format(date)}"
        id = "${dateId}_${dimensionId}"
        data[DATE_ID] = dateId
        data[DIMENSION_ID] = dimensionId
        data.remove DAY_OF_MONTH
        data.remove WEEK_OF_YEAR
        data.remove HOUR_OF_DAY
        data.remove MINUTE
    }

    void prepareForDay() {
        dateId = "${DsHelper.formatIdDay.format(date)}"
        id = "${dateId}_${dimensionId}"
        data[DATE_ID] = dateId
        data[DIMENSION_ID] = dimensionId
        data.remove HOUR_OF_DAY
        data.remove MINUTE
    }

    void prepareForMinute() {
        dateId = "${DsHelper.formatIdMinute.format(date)}"
        id = "${dateId}_${dimensionId}"
        data[DATE_ID] = dateId
        data[DIMENSION_ID] = dimensionId
    }
}
