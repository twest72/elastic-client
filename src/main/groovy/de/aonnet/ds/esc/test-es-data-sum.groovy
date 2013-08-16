package de.aonnet.ds.esc

import groovy.json.JsonOutput
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.StopWatch
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet
import org.elasticsearch.search.facet.statistical.StatisticalFacet

import java.text.SimpleDateFormat

/**
 * elasticsearch test client
 *
 * User: westphal
 * Date: 17.07.13
 * Time: 21:39
 */

StopWatch stopWatch = new StopWatch()

// connect to server and create groovy client
Client javaClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress('localhost', 9300));
client = new GClient(javaClient)

refresh = {
    RefreshResponse actionGet = client.admin.indices.prepareRefresh().execute().actionGet()
    if (actionGet.shardFailures.length < 0) {
        throw new IllegalStateException("refresh failed: ${actionGet.shardFailures}")
    }
}

String indexDataSumTest = 'data_sum_test'
String type = 'data1'

// delete and create index
try {
    client.admin.indices.prepareDelete(indexDataSumTest).execute().actionGet();
} catch (IndexMissingException e) {}
client.admin.indices.create(new CreateIndexRequest(indexDataSumTest)).actionGet()

// define mapping
Map mapping = [
        (type): [
                properties: [
                        adwords_campaigns: [
                                type: 'string'
                        ],
                        average_cpm: [
                                type: 'double'
                        ],
                        average_cpc: [
                                type: 'double'
                        ],
                        clicks: [
                                type: 'long',
                        ],
                        conversions: [
                                type: 'long',
                        ],
                        cost: [
                                type: 'double',
                        ],
                        date: [
                                type: 'date'
                        ],
                ],
        ]
]

client.admin.indices.putMapping(new PutMappingRequest(indexDataSumTest).type(type).source(mapping)).actionGet()

// create
List<Map<String, ?>> dataList = [
        [
                adwords_campaigns: 'Krankenkassen Online',
                average_cpm: 30.14,
                average_cpc: 0.34,
                clicks: 933,
                conversions: 42,
                cost: 26.52,
                date: '2013-02-11T00:00:00Z',
        ],
        [
                adwords_campaigns: 'Brand - Krankenkassenportal',
                average_cpm: 20.14,
                average_cpc: 0.04,
                clicks: 512,
                conversions: 22,
                cost: 23.32,
                date: '2013-02-12T23:12:00Z',
        ],
        [
                adwords_campaigns: 'Contentkampagne',
                average_cpm: 0.16,
                average_cpc: 0.55,
                clicks: 289,
                conversions: 0,
                cost: 158.62,
                date: '2013-02-12T23:13:01Z',
        ],
        [
                adwords_campaigns: 'Contentkampagne 2',
                average_cpm: 0.13,
                average_cpc: 0.54,
                clicks: 23,
                conversions: 1,
                cost: 158.33,
                date: '2013-02-12T23:13:02Z',
        ],
        [
                adwords_campaigns: 'Generisch - Krankenkassen',
                average_cpm: 63.03,
                average_cpc: 0.93,
                clicks: 5127,
                conversions: 45,
                cost: 4766.34,
                date: '2013-03-13T00:00:00Z',
        ]
]

println 'generate data'
stopWatch.start 'generate data'
Date dateA = Date.parse("dd-MM-yyyy", "01-06-2012")
int range = 365
SimpleDateFormat format = new SimpleDateFormat()
format.setTimeZone(TimeZone.getTimeZone('UTC'))
SimpleDateFormat formatElasticSearch = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ")
formatElasticSearch.setTimeZone(TimeZone.getTimeZone('UTC'))

1000.times {
    def randomInterval = new Random().nextInt(range)
    Date date = dateA.plus(randomInterval)
    String dateString = formatElasticSearch.format(date)
    dataList << [
            adwords_campaigns: "Generische Daten $it",
            average_cpm: Math.random() * 100,
            average_cpc: Math.random(),
            clicks: Math.random() * 1000 as int,
            conversions: Math.random() * 100 as int,
            cost: Math.random() * 1000,
            date: dateString,
    ]
}
stopWatch.stop()
println 'generate data completed'

println 'add data to store'
stopWatch.start 'add data to store'
dataList.eachWithIndex { Map<String, ?> data, int id ->
    String jsonData = JsonOutput.toJson(data)
    IndexResponse indexResponse = client.prepareIndex(indexDataSumTest, type)
            .setId(id as String)
            .setSource(jsonData)
            .execute()
            .actionGet();

    assert indexResponse.index == indexDataSumTest
    assert indexResponse.type == type
    assert indexResponse.id == id as String
}
stopWatch.stop()
println 'add data to store completed'

println 'refresh'
stopWatch.start 'refresh'
refresh()
stopWatch.stop()
println 'refresh completed'

// search all from index
println 'search all from index'
stopWatch.start 'search all from index'
SearchResponse searchAllResponse = client.prepareSearch(indexDataSumTest).setTypes(type).execute().actionGet();
stopWatch.stop()
println 'search all from index completed'

assert searchAllResponse.failedShards == 0

println "search all -> totalHits:   ${searchAllResponse.hits.totalHits}"
searchAllResponse.hits.eachWithIndex { SearchHit searchHit, Integer index ->
    println "search all -> hit $index (score $searchHit.score): $searchHit.source"
}

// search all from index
String statClicks = 'stat_clicks'
String histClicksByDay = 'hist_clicks_day'
String histClicksByMinute = 'hist_clicks_minute'
String histClicksByMonth = 'hist_clicks_month'
SearchRequestBuilder searchRequest = client
        .prepareSearch(indexDataSumTest)
        .setSearchType(SearchType.QUERY_THEN_FETCH)
        .setQuery(QueryBuilders.matchAllQuery())
//        .setFilter(contribRangeFilter)
        .addFacet(FacetBuilders.statisticalFacet(statClicks).field('clicks'))
        .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMinute).keyField('date').valueField('clicks').interval('minute'))
        .addFacet(FacetBuilders.dateHistogramFacet(histClicksByDay).keyField('date').valueField('clicks').interval('day'))
        .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMonth).keyField('date').valueField('clicks').interval('month'))
//        .setFrom(0)
//        .setSize(100)
        .addFields('adwords_campaigns', 'clicks')


SearchResponse searchWithFacetResponse = searchRequest.execute().actionGet();

assert searchWithFacetResponse.failedShards == 0

println "search all -> totalHits:   ${searchWithFacetResponse.hits.totalHits}"
searchWithFacetResponse.hits.eachWithIndex { SearchHit searchHit, Integer index ->

    String fields = searchHit.fields.collect { "${it.key} (${it.value.name}): ${it.value.value}" }.join(', ')
    println "search all -> hit $index (score $searchHit.score): $fields"

}
StatisticalFacet statClicksFacet = searchWithFacetResponse.facets.facet(StatisticalFacet, statClicks)
['count', 'min', 'max', 'total', 'variance'].each { String propertyName ->
    println "search all -> facet $statClicks, $propertyName: ${statClicksFacet.properties[propertyName]}"
}

DateHistogramFacet histClicksByMonthFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByMonth)
histClicksByMonthFacet.entries.each { DateHistogramFacet.Entry entry ->
    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
        String date = format.format(new Date(entry.time))
        println "search all -> facet $histClicksByMonth, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
    }
}

DateHistogramFacet histClicksByDayFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByDay)
histClicksByDayFacet.entries.each { DateHistogramFacet.Entry entry ->
    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
        String date = format.format(new Date(entry.time))
        println "search all -> facet $histClicksByDay, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
    }
}

DateHistogramFacet histClicksByMinuteFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByMinute)
histClicksByMinuteFacet.entries.each { DateHistogramFacet.Entry entry ->
    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
        String date = format.format(new Date(entry.time))
        println "search all -> facet $histClicksByMinute, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
    }
}

println stopWatch.prettyPrint()

//def getContributionsByCandName = { String candName, Double amtEqGtThan ->
//    QueryBuilder matchQuery = QueryBuilders.matchQuery("candNm", candName);
//    FilterBuilder contribRangeFilter = FilterBuilders.rangeFilter(
//            "contbReceiptAmt").gte(amtEqGtThan);
//    StatisticalFacetBuilder facet = FacetBuilders.statisticalFacet("stat1")
//            .field("contbReceiptAmt");
//    SearchRequestBuilder request = client
//            .prepareSearch("contributions")
//            .addSort(
//            SortBuilders.fieldSort("contbReceiptAmt").order(
//                    SortOrder.DESC))
//            .setSearchType(SearchType.QUERY_THEN_FETCH)
//            .setQuery(matchQuery)
//            .setFilter(contribRangeFilter)
//            .addFacet(facet)
//            .setFrom(0)
//            .setSize(100)
//            .addFields("contbrNm", "candNm", "contbrEmployer",
//            "contbReceiptAmt");
//    System.out.println("SEARCH QUERY: " + request.toString());
//
//    SearchResponse response = request.execute().actionGet();
//    SearchHits searchHits = response.getHits();
//    SearchHit[] hits = searchHits.getHits();
//    for (SearchHit hit : hits) {
//        Map<String, SearchHitField> fields = hit.getFields();
//        System.out.println(hit.getId() + ", contbrEmployer="
//                + fields.get("contbrEmployer").getValue().toString());
//    }
//}
//{
//           "query" : {
//               "match_all" : {}
//           },
//           "facets" : {
//               "stat1" : {
//                   "statistical" : {
//                       "field" : "num1"
//                   }
//               }
//           }
//       }
