package de.aonnet.ds.esc

import groovy.transform.ToString

/**
 * User: westphal
 * Date: 13.08.13
 * Time: 09:50
 */
@ToString(includeNames = true, includePackage = false)
class TimeSpan {

    Date from
    Date to
    TimeSpanType timeSpanType
}
