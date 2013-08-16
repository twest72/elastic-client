package de.aonnet.ds.esc

/**
 * User: westphal
 * Date: 13.08.13
 * Time: 09:50
 */
enum TimeSpanType {

    MINUTE('minute'),
    DAY('day'),
    MONTH('month')

    String elasticType

    TimeSpanType(String elasticType) {
        this.elasticType = elasticType
    }
}
