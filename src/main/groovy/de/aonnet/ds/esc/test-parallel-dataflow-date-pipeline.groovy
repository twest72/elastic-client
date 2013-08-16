package de.aonnet.ds.esc

import groovy.json.JsonOutput
import groovyx.gpars.GParsPool
import groovyx.gpars.dataflow.DataflowQueue
import groovyx.gpars.dataflow.DataflowReadChannel
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHit

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import static groovyx.gpars.dataflow.Dataflow.task
import static org.elasticsearch.index.query.QueryBuilders.idsQuery

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

SimpleDateFormat formatElasticSearch = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ")
formatElasticSearch.setTimeZone(TimeZone.getTimeZone('UTC'))
int dataGenerationCount = 1000
Date dateA = Date.parse('dd-MM-yyyy', '01-06-2012')
int dayRange = 50
String indexPrefix = 'test-cust'
String testDimension = 'testDimension'
String typeMinute = 'minute'
String typeDay = 'day'
String typeMonth = 'month'


Closure<GString> createIndex = { String dimension ->
    return "${indexPrefix}_${dimension.toLowerCase()}"
}

String testIndex = createIndex testDimension

Client javaClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress('localhost', 9300));
client = new GClient(javaClient)

//// delete and create index
//try {
//    client.admin.indices.prepareDelete(testIndex).execute().actionGet();
//} catch (IndexMissingException e) {}
//client.admin.indices.create(new CreateIndexRequest(testIndex)).actionGet()

// define mapping
[typeMinute, typeDay, typeMonth].each { String type ->
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

    client.admin.indices.putMapping(new PutMappingRequest(testIndex).type(type).source(mapping)).actionGet()
}
Closure<ClusterHealthStatus> waitForRelocation = { String index ->
    println 'wait for relocation'
    ClusterHealthRequest request = Requests.clusterHealthRequest(index).waitForRelocatingShards(0);
    ClusterHealthResponse actionGet = client.admin.cluster.health(request).actionGet();
    assert !actionGet.isTimedOut()
    return actionGet.getStatus();
}

Closure<RefreshResponse> refresh = { String index ->
    println 'wait for refresh'
    RefreshResponse actionGet = client.admin.indices.prepareRefresh(index).execute().actionGet();
    assert actionGet.failedShards == 0
    return actionGet;
}

Closure waitForGreenStatus = { String index ->
    println 'wait for green'
    ClusterHealthRequestBuilder healthRequest = client.admin.cluster.prepareHealth(index).setWaitForGreenStatus()
    ClusterHealthResponse healthResponse = healthRequest.execute().actionGet()
    println "status: $healthResponse.status"

    waitForRelocation(index);
    refresh(index);
    println 'green'
}

Closure<? extends Map<String, ? extends Object>> saveToDs = { String index, String type, List<DsImportData> importDataList ->

    BulkRequestBuilder bulkRequest = client.client.prepareBulk()
    importDataList.each { DsImportData importData ->
        String jsonData = JsonOutput.toJson(importData.data)
        bulkRequest.add(
                client.prepareIndex(index, type)
                        .setId(importData.id as String)
                        .setSource(jsonData)
        )

    }

    println "step execute insert"
    BulkResponse bulkResponse = bulkRequest.execute().actionGet()
    println "step insert ${bulkResponse.items.size()} items"
    Map<String, Object> returnMessage = [:]
    returnMessage.success = bulkResponse.hasFailures()
    if (!returnMessage.success) {
        returnMessage.errorMessage = bulkResponse.buildFailureMessage()
    }
    returnMessage.importDataList = importDataList

    return returnMessage
}


Closure<SearchHit> readFromDs = { String index, String type, String id ->

    waitForGreenStatus(index)
    SearchRequestBuilder searchRequest = client
            .prepareSearch(index)
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setQuery(idsQuery(type).ids(id))
    SearchResponse searchWithFacetResponse = searchRequest.execute().actionGet()

    if (searchWithFacetResponse.hits.totalHits == 0) {
        return null
    }
    if (searchWithFacetResponse.hits.totalHits == 1) {
        return searchWithFacetResponse.hits.hits[0]
    }

    throw new IllegalStateException('1 expected')
}

Closure<DsImportData> prepareData = { DsImportData importData ->

    Calendar dateACalendar = GregorianCalendar.newInstance()
    dateACalendar.setTime(dateA)
    dateACalendar.add(Calendar.SECOND, new Random().nextInt(60 * 60 * 24 * dayRange))
    importData.date = dateACalendar.getTime()
    importData.year = dateACalendar.get(Calendar.YEAR)
    importData.month = dateACalendar.get(Calendar.MONTH)
    importData.dayOfMonth = dateACalendar.get(Calendar.DAY_OF_MONTH)
    importData.weekOfYear = dateACalendar.get(Calendar.WEEK_OF_YEAR)
    importData.hourOfDay = dateACalendar.get(Calendar.HOUR_OF_DAY)
    importData.minute = dateACalendar.get(Calendar.MINUTE)
    importData.second = dateACalendar.get(Calendar.SECOND)
    Map<String, ? extends Object> map = [
            adwords_campaigns: "Generische Daten ${importData.id}",
            average_cpm: Math.random() * 100,
            average_cpc: Math.random(),
            clicks: Math.random() * 1000 as int,
            conversions: Math.random() * 100 as int,
            cost: Math.random() * 1000,
            date: formatElasticSearch.format(importData.date),
            counter: 1,
            year: importData.year,
            month: importData.month,
            day_of_month: importData.dayOfMonth,
            week_of_year: importData.weekOfYear,
            hour_of_day: importData.hourOfDay,
            minute: importData.minute,
    ]
    importData.data.putAll(map)
    return importData
}

Closure mergeData = { Map<String, Object> newData, Map<String, Object> currentData ->
    newData.average_cpm = newData.average_cpm + currentData.average_cpm
    newData.average_cpc = newData.average_cpc + currentData.average_cpc
    newData.clicks = newData.clicks + currentData.clicks
    newData.conversions = newData.conversions + currentData.conversions
    newData.cost = newData.cost + currentData.cost
    newData.counter = (currentData['counter'] ?: 0) + 1
}

Closure<DsImportData> saveMinuteData = { DsImportData importDataOriginal ->

    DsImportData importData = importDataOriginal.clone()

    String index = createIndex(importData.dimension)
    String type = typeMinute
    importData.prepareForMinute()

    SearchHit searchHit = readFromDs(index, type, importData.id)

    if (searchHit) {
        mergeData(importData.data, searchHit.source)
    }

    println "Save in index $index, type $type: $importData"
    saveToDs index, type, [importData]
    return importDataOriginal
}

Closure<DsImportData> saveDayData = { DsImportData importDataOriginal ->

    DsImportData importData = importDataOriginal.clone()

    String index = createIndex(importData.dimension)
    String type = typeDay
    importData.prepareForDay()

    SearchHit searchHit = readFromDs(index, type, importData.id)

    if (searchHit) {
        mergeData(importData.data, searchHit.source)
    }

    println "Save in index $index, type $type: $importData"
    saveToDs index, type, [importData]
    return importDataOriginal
}

Closure<DsImportData> saveMonthDayData = { DsImportData importDataOriginal ->

    DsImportData importData = importDataOriginal.clone()

    String index = createIndex(importData.dimension)
    String type = typeMonth
    importData.prepareForMonth()

    SearchHit searchHit = readFromDs(index, type, importData.id)

    if (searchHit) {
        mergeData(importData.data, searchHit.source)
    }

    println "Save in index $index, type $type: $importData"
    saveToDs index, type, [importData]
    return importDataOriginal
}


DataflowQueue<DsImportData> inputMinuteDataQueue = new DataflowQueue<DsImportData>()
DataflowQueue<DsImportData> saveMinuteDataQueue = new DataflowQueue<DsImportData>()
DataflowQueue<DsImportData> saveDayDataQueue = new DataflowQueue<DsImportData>()
DataflowQueue<DsImportData> saveMonthDataQueue = new DataflowQueue<DsImportData>()

inputMinuteDataQueue.chainWith(prepareData).split([saveMinuteDataQueue, saveDayDataQueue, saveMonthDataQueue])

List<DataflowReadChannel> reader = []
reader << saveMinuteDataQueue.chainWith(saveMinuteData)
reader << saveDayDataQueue.chainWith(saveDayData)
reader << saveMonthDataQueue.chainWith(saveMonthDayData)

dataGenerationCount.times {
    inputMinuteDataQueue << new DsImportData(dimensionId: '4711', dimension: testDimension, data: [description: "Data $it"])
}

def taskRunner = { String name, DataflowReadChannel r ->
    println "---- start task $name"
    String lastValue = ''
    try {
        task {
            while (true) {
                lastValue = r.val
                println "-- ${lastValue}"
            }
        }.get(5, TimeUnit.MINUTES)
    } catch (TimeoutException e) {
        println "---- end task $name with last value $lastValue"
    }
}

GParsPool.withPool {
    println "---- start tasks"
    reader.eachWithIndexParallel { DataflowReadChannel r, Integer index ->
        taskRunner(index as String, r)
    }
    println "---- end tasks"
}
