package de.aonnet.ds.esc

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequestBuilder
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.search.SearchRequestBuilder
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.index.query.FilterBuilder
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.facet.FacetBuilders
import org.elasticsearch.search.facet.statistical.StatisticalFacet

import java.text.SimpleDateFormat

/**
 * User: westphal
 * Date: 07.08.13
 * Time: 08:40
 */

SimpleDateFormat formatElasticSearch = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZ")
formatElasticSearch.setTimeZone(TimeZone.getTimeZone('UTC'))
String indexPrefix = 'test-cust'
String testDimension = 'testDimension'

Closure<GString> createIndex = { String dimension ->
    return "${indexPrefix}_${dimension.toLowerCase()}"
}

String testIndex = createIndex testDimension

Client javaClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress('localhost', 9300));
client = new GClient(javaClient)

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

String statClicks = 'stat_clicks'

Closure<SearchResponse> readFromDsFromTo = { String index, Date from, Date to, String dimensionId = null ->

    waitForGreenStatus(index)

    FilterBuilder f = TimeSpanFilterHelper.create(from, to, dimensionId)
    SearchRequestBuilder searchRequest = client
            .prepareSearch(index)
            .setSearchType(SearchType.QUERY_AND_FETCH)
            .setFilter(f)
            .addFacet(FacetBuilders.statisticalFacet(statClicks).field('clicks').facetFilter(f))

    return searchWithFacetResponse = searchRequest.execute().actionGet()
}
100.times {
    SearchResponse searchResponse = readFromDsFromTo testIndex, DsHelper.formatIdMinute.parse('20120720_1702'), DsHelper.formatIdMinute.parse('20120720_2011')
    println "search minutes -> hits    ${searchResponse.hits.totalHits}"
    println "search minutes -> counter ${searchResponse.hits*.source['counter'].sum()}"
    StatisticalFacet statClicksFacet = searchResponse.facets.facet(StatisticalFacet, statClicks)
    ['count', 'min', 'max', 'mean', 'total', 'variance'].each { String propertyName ->
        println "search minutes -> facet   $statClicks, $propertyName: ${statClicksFacet.properties[propertyName]}"
    }
    searchResponse = readFromDsFromTo testIndex, DsHelper.formatIdMinute.parse('20120720_1702'), DsHelper.formatIdMinute.parse('20120720_2011'), '4711'
    println "search minutes with dimension id -> hits    ${searchResponse.hits.totalHits}"
    println "search minutes with dimension id -> counter ${searchResponse.hits*.source['counter'].sum()}"
    statClicksFacet = searchResponse.facets.facet(StatisticalFacet, statClicks)
    ['count', 'min', 'max', 'mean', 'total', 'variance'].each { String propertyName ->
        println "search minutes with dimension id -> facet   $statClicks, $propertyName: ${statClicksFacet.properties[propertyName]}"
    }

    searchResponse = readFromDsFromTo testIndex, DsHelper.formatIdDay.parse('20120719'), DsHelper.formatIdDay.parse('20120720')
    println "search days -> hits    ${searchResponse.hits.totalHits}"
    println "search days -> counter ${searchResponse.hits*.source['counter'].sum()}"
    statClicksFacet = searchResponse.facets.facet(StatisticalFacet, statClicks)
    ['count', 'min', 'max', 'mean', 'total', 'variance'].each { String propertyName ->
        println "search days -> facet   $statClicks, $propertyName: ${statClicksFacet.properties[propertyName]}"
    }

}
