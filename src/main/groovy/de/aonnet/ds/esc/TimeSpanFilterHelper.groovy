package de.aonnet.ds.esc

import org.elasticsearch.index.query.AndFilterBuilder
import org.elasticsearch.index.query.BoolQueryBuilder
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.FilterBuilders
import org.elasticsearch.index.query.OrFilterBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.RangeFilterBuilder

import java.text.SimpleDateFormat

/**
 * User: westphal
 * Date: 13.08.13
 * Time: 08:45
 */
class TimeSpanFilterHelper {

    static FilterBuilder create(Date from, Date to, String dimensionId = null) {

        TimeSpanCalculator calculator = new TimeSpanCalculator(from, to)

        List<TimeSpan> timeSpans = calculator.timeSpans

        OrFilterBuilder orFilterBuilder = new OrFilterBuilder()
        timeSpans.each { TimeSpan timeSpan ->

            AndFilterBuilder timeSpanFilter = new AndFilterBuilder()

            SimpleDateFormat format = DsHelper.getFormatForTimeSpanType(timeSpan.timeSpanType)
            String idFrom = format.format(from)
            String idTo = format.format(to)

            timeSpanFilter.add FilterBuilders.rangeFilter(DsImportData.DATE_ID).from(idFrom).to(idTo)
            timeSpanFilter.add FilterBuilders.termFilter('_type', timeSpan.timeSpanType.elasticType)

            if(dimensionId) {
                timeSpanFilter.add FilterBuilders.termFilter(DsImportData.DIMENSION_ID, dimensionId)
            }

            orFilterBuilder.add timeSpanFilter
        }
        return orFilterBuilder
    }
}
