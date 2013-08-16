package de.aonnet.ds.esc

import groovy.json.JsonOutput
import groovyx.gpars.actor.DefaultActor
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
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

// connect to server and create groovy client
Client javaClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress('localhost', 9300));
client = new GClient(javaClient)

String indexDataSumTest = 'data_sum_test'
String type = 'data1'
int dataSize = 10000000
int bulkSize = 50000

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

abstract class DsActor extends DefaultActor {

    GClient client
    String index
    String type
    StopWatch stopWatch
    int dataSize
    SimpleDateFormat formatElasticSearch
    SimpleDateFormat formatOutput

    void afterStart() {
        formatElasticSearch = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ")
        formatElasticSearch.setTimeZone(TimeZone.getTimeZone('UTC'))
        formatOutput = new SimpleDateFormat('yyyy-MM-dd hh:mm:ss')
        formatOutput.setTimeZone(TimeZone.getTimeZone('UTC'))
    }
}

class DsWriter extends DsActor {

    int bulkSize

    void afterStart() {
        super.afterStart()
        stopWatch = new StopWatch('DsWriter')
    }

    void act() {
        // create data

        println 'generate data'
        stopWatch.start 'generate data'
        Date dateA = Date.parse('dd-MM-yyyy', '01-06-2012')
        int range = 365

        int bulkSteps = dataSize / bulkSize
        int id = 1
        bulkSteps.times { int bulkStep ->

            BulkRequestBuilder bulkRequest = client.client.prepareBulk()
            bulkSize.times {
                def randomInterval = new Random().nextInt(range)
                Date date = dateA.plus(randomInterval)
                String dateString = formatElasticSearch.format(date)
                Map<String, ?> data = [
                        adwords_campaigns: "Generische Daten $id",
                        average_cpm: Math.random() * 100,
                        average_cpc: Math.random(),
                        clicks: Math.random() * 1000 as int,
                        conversions: Math.random() * 100 as int,
                        cost: Math.random() * 1000,
                        date: dateString,
                ]
                String jsonData = JsonOutput.toJson(data)
                bulkRequest.add(client.prepareIndex(index, type)
                        .setId(id as String)
                        .setSource(jsonData))
                id++
            }

            println "step $bulkStep execute insert"
            BulkResponse bulkResponse = bulkRequest.execute().actionGet()
            println "step $bulkStep insert ${bulkResponse.items.size()} items"
            assert !bulkResponse.hasFailures()
            bulkRequest = client.client.prepareBulk()
        }

        stopWatch.stop()
        println 'add data to store completed'

        println stopWatch.shortSummary()
        terminate()
    }
}

class DsReader extends DsActor {

    org.apache.commons.lang.time.StopWatch stopWatchRefresh
    org.apache.commons.lang.time.StopWatch stopWatchSearch

    void afterStart() {
        super.afterStart()
        stopWatch = new StopWatch('DsReader')
        stopWatchRefresh = new org.apache.commons.lang.time.StopWatch()
        stopWatchSearch = new org.apache.commons.lang.time.StopWatch()
    }

    void act() {
        stopWatch.start 'all'
        stopWatchRefresh.start()
        stopWatchRefresh.suspend()
        stopWatchSearch.start()
        stopWatchSearch.suspend()
        loop {
            Thread.sleep(10000)
            stopWatchRefresh.resume()
            refresh()
            stopWatchRefresh.suspend()

            // search all from index
            String statClicks = 'stat_clicks'
            String histClicksByDay = 'hist_clicks_day'
            String histClicksByMinute = 'hist_clicks_minute'
            String histClicksByMonth = 'hist_clicks_month'
            int page = 0
            SearchRequestBuilder searchRequest = client
                    .prepareSearch(index)
                    .setSearchType(SearchType.QUERY_THEN_FETCH)
                    .setQuery(QueryBuilders.matchAllQuery())
                    .addFacet(FacetBuilders.statisticalFacet(statClicks).field('clicks'))
                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMinute).keyField('date').valueField('clicks').interval('minute'))
                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByDay).keyField('date').valueField('clicks').interval('day'))
                    .addFacet(FacetBuilders.dateHistogramFacet(histClicksByMonth).keyField('date').valueField('clicks').interval('month'))
                    .addFields('adwords_campaigns', 'clicks')
                    .setFrom(page).setSize(1000)


            stopWatchSearch.resume()
            SearchResponse searchWithFacetResponse = searchRequest.execute().actionGet();
            stopWatchSearch.suspend()

            assert searchWithFacetResponse.failedShards == 0

            println "search all: load page $page with size ${searchWithFacetResponse.hits.hits.size()} from ${searchWithFacetResponse.hits.totalHits} hits"

            if (dataSize <= searchWithFacetResponse.hits.totalHits) {
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
                        String date = formatOutput.format(new Date(entry.time))
                        println "search all -> facet $histClicksByMonth, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
                    }
                }

//                DateHistogramFacet histClicksByDayFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByDay)
//                histClicksByDayFacet.entries.each { DateHistogramFacet.Entry entry ->
//                    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
//                        String date = formatOutput.formatOutput(new Date(entry.time))
//                        println "search all -> facet $histClicksByDay, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
//                    }
//                }
//
//                DateHistogramFacet histClicksByMinuteFacet = searchWithFacetResponse.facets.facet(DateHistogramFacet, histClicksByMinute)
//                histClicksByMinuteFacet.entries.each { DateHistogramFacet.Entry entry ->
//                    ['count', 'min', 'max', 'total', 'totalCount'].each { String propertyName ->
//                        String date = formatOutput.formatOutput(new Date(entry.time))
//                        println "search all -> facet $histClicksByMinute, Entry  ${date}, $propertyName: ${entry.properties[propertyName]}"
//                    }
//                }

                stopWatch.stop()
                stopWatchRefresh.stop()
                stopWatchSearch.stop()

                println "refresh time: $stopWatchRefresh"
                println "search time: $stopWatchSearch"
                println stopWatch.shortSummary()

                terminate()
            }
        }
    }

    private refresh() {
        RefreshResponse actionGet = client.admin.indices.prepareRefresh().execute().actionGet()
        if (actionGet.shardFailures.length < 0) {
            throw new IllegalStateException("refresh failed: ${actionGet.shardFailures}")
        }
    }
}

def master = new DsWriter(client: client, index: indexDataSumTest, type: type, dataSize: dataSize, bulkSize: bulkSize).start()
def player = new DsReader(client: client, index: indexDataSumTest, type: type, dataSize: dataSize).start()

//this forces main thread to live until both actors stop
[master, player]*.join()


