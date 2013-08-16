package de.aonnet.ds.esc

import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.StopWatch
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet
import org.elasticsearch.search.facet.statistical.StatisticalFacet

/**
 * elasticsearch test client
 *
 * User: westphal
 * Date: 17.07.13
 * Time: 21:39
 */

// connect to server and create groovy client
Map<String, String> settingsMap = ['discovery.logEnabled': true as String, 'client.transport.sniff': true as String]
Settings settings = ImmutableSettings.settingsBuilder().put(settingsMap).build()
Client javaClient = new TransportClient(settings)
        .addTransportAddress(new InetSocketTransportAddress('localhost', 9300))
        .addTransportAddress(new InetSocketTransportAddress('localhost', 9301))
client = new GClient(javaClient)

String indexDataSumTest = 'data_sum_test'
String type = 'data1'
int dataSize = 10000000
int bulkSize = 50000


class DsParallelReader extends DsActor {

    org.apache.commons.lang.time.StopWatch stopWatchSearch
    int readTo

    void afterStart() {
        super.afterStart()
        stopWatch = new StopWatch('DsReader')
        stopWatchSearch = new org.apache.commons.lang.time.StopWatch()
    }

    void act() {
        stopWatch.start 'all'
        stopWatchSearch.start()
        stopWatchSearch.suspend()
        int counter = 0
        loop {
            Thread.sleep(1000)

            // search all from index
            String statClicks = 'stat_clicks'
            String histClicksByDay = 'hist_clicks_day'
            String histClicksByMinute = 'hist_clicks_minute'
            String histClicksByMonth = 'hist_clicks_month'
            Date from = Date.parse('dd-MM-yyyy', '01-12-2012')
            Date to = Date.parse('dd-MM-yyyy', '01-02-2013')
            int page = 0
            SearchRequestBuilder searchRequest = client
                    .prepareSearch(index)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.rangeQuery('date').from(formatElasticSearch.format(from)).to(formatElasticSearch.format(to)))
                    .addFacet(FacetBuilders.statisticalFacet(statClicks).field('clicks'))
//                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMinute).keyField('date').valueField('clicks').interval('minute'))
                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByDay).keyField('date').valueField('clicks').interval('day'))
                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMonth).keyField('date').valueField('clicks').interval('month'))
                    .addFields('adwords_campaigns', 'clicks')
                    .setFrom(page).setSize(100)


            stopWatchSearch.resume()
            SearchResponse searchWithFacetResponse = searchRequest.execute().actionGet();
            stopWatchSearch.suspend()

            assert searchWithFacetResponse.failedShards == 0

            println "search all: load page $page with size ${searchWithFacetResponse.hits.hits.size()} from ${searchWithFacetResponse.hits.totalHits} hits"

            if (readTo == counter) {
                searchWithFacetResponse.hits.eachWithIndex { SearchHit searchHit, Integer index ->

                    String fields = searchHit.fields.collect { "${it.key} (${it.value.name}): ${it.value.value}" }.join(', ')
                    println "search all -> hit $index (score $searchHit.score): $fields"

                }
                if (searchWithFacetResponse.facets) {
                    StatisticalFacet statClicksFacet = searchWithFacetResponse.facets.facet(StatisticalFacet, statClicks)
                    ['count', 'min', 'max', 'total', 'variance'].each { String propertyName ->
                        println "search all -> facet $statClicks, $propertyName: ${statClicksFacet.properties[propertyName]}"
                    }

                    DateHistogramFacet histClicksByMonthFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByMonth)
                    histClicksByMonthFacet.entries.each { DateHistogramFacet.Entry entry ->
                        ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
                            String date = formatOutput.format(new Date(entry.time))
                            println "search all -> facet $histClicksByMonth, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
                        }
                    }

                    DateHistogramFacet histClicksByDayFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByDay)
                    histClicksByDayFacet.entries.each { DateHistogramFacet.Entry entry ->
                        ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
                            String date = formatOutput.format(new Date(entry.time))
                            println "search all -> facet $histClicksByDay, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
                        }
                    }

//                DateHistogramFacet histClicksByMinuteFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByMinute)
//                histClicksByMinuteFacet.entries.each { DateHistogramFacet.Entry entry ->
//                    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
//                        String date = formatOutput.format(new Date(entry.time))
//                        println "search all -> facet $histClicksByMinute, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
//                    }
//                }

                }

                stopWatch.stop()
                stopWatchSearch.stop()

                println "search time: $stopWatchSearch"
                println stopWatch.shortSummary()

                terminate()
            }
            counter++
        }
    }
}

List<DsParallelReader> players = []
players << new DsParallelReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, readTo: 1)
//players << new DsParallelReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, readTo: 1000)
//players << new DsParallelReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, readTo: 1000)
//players << new DsParallelReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, readTo: 1000)
//players << new DsParallelReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, readTo: 1000)

players.each {
    it.start()
    Thread.sleep(50)
}

//this forces main thread to live until both actors stop
players*.join()

client.client.close()
