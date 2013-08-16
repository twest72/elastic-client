package de.aonnet.ds.esc

import groovy.json.JsonOutput
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse
import org.elasticsearch.action.admin.cluster.health.ClusterHealthStatus
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.Client
import org.elasticsearch.client.Requests
import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.groovy.client.GClient
import org.elasticsearch.groovy.client.action.GActionFuture
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.indices.IndexMissingException
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * elasticsearch test client
 *
 * User: westphal
 * Date: 17.07.13
 * Time: 21:39
 */

// on startup
//GNodeBuilder nodeBuilder = new GNodeBuilder()
//nodeBuilder.settings {
//    node {
//        local = true
//    }
//    gateway {
//        type = 'none'
//    }
//}
//node = nodeBuilder.node()
//GClient client = node.client

// connect to server and create groovy client
Client javaClient = new TransportClient().addTransportAddress(new InetSocketTransportAddress('localhost', 9300));
client = new GClient(javaClient)

refresh = {
RefreshResponse actionGet = client.admin.indices.prepareRefresh().execute().actionGet()
    if(actionGet.shardFailures.length < 0) {
        throw new IllegalStateException("refresh failed: ${actionGet.shardFailures}")
    }
}

String idx_test = 'idx_test'
try {
    client.admin.indices.prepareDelete(idx_test).execute().actionGet();
} catch (IndexMissingException e) {}
client.admin.indices.create(new CreateIndexRequest(idx_test)).actionGet()

// Make it nested
String json = JsonOutput.toJson([
        type1: [
                properties: [
                        user: [
                                type: "string"
                        ],
                        postDate: [
                                type: "string"
                        ],
                        message: [
                                type: "string"
                        ],
                        complex: [
                                type: "nested",
                        ],
                        detail: [
                                type: "nested"
                        ],
                ],
        ]
])

// TODO test map
client.admin.indices.putMapping(new PutMappingRequest(idx_test).type('type1').source(json)).actionGet()
json = JsonOutput.toJson([
        type1: [
                properties: [
                        detail: [
                                type: "nested"
                        ],
                ],
        ]
])
client.admin.indices.putMapping(new PutMappingRequest(idx_test).type('type1').source(json)).actionGet()

refresh()

// index with groovy
json = JsonOutput.toJson([
        user: 'picard',
        postDate: '2013-03-12',
        message: 'trying out GClient',
        complex: [
                detail: [
                        color: 'red',
                        age: 12
                ]
        ]
])

GActionFuture<IndexResponse> indexResponseAction = client.index {
    index idx_test
    type 'type1'
    id '1'
    source json
}

assert indexResponseAction.response.index == idx_test
assert indexResponseAction.response.type == 'type1'
assert indexResponseAction.response.id == '1'

// index with java
json = JsonOutput.toJson([
        user: 'kimchy',
        postDate: '2013-03-12',
        message: 'trying out Elastic Search',
        complex: [
                detail: [
                        color: 'green',
                        age: 12
                ]
        ]
])

IndexResponse indexResponse = client.prepareIndex(idx_test, "type1")
        .setId('2')
        .setSource(json)
        .execute()
        .actionGet();

assert indexResponse.index == idx_test
assert indexResponse.type == 'type1'
assert indexResponse.id == '2'

// get
GActionFuture<GetResponse> getF = client.get {
    index idx_test
    type 'type1'
    id '1'
}
GetResponse getResponse = getF.response
assert getResponse.exists
assert getResponse.id == '1'
assert getResponse.index == idx_test
assert getResponse.type == 'type1'
assert getResponse.fields == [:]
assert getResponse.source == [message: 'trying out GClient', complex: [detail: [color: 'red', age: 12]], postDate: '2013-03-12', user: 'picard']

refresh()

// search all
SearchResponse searchAllResponse = client.prepareSearch().execute().actionGet();

assert searchAllResponse.failedShards == 0

println "search all -> totalHits:   ${searchAllResponse.hits.totalHits}"
searchAllResponse.hits.eachWithIndex { SearchHit searchHit, Integer index ->
    println "search all -> hit $index (score $searchHit.score): $searchHit.source"
}

// search kimchy (java like)
SearchResponse searchResponse = client.prepareSearch("idx_test").setTypes('type1')
        .setQuery([term: [user: "kimchy"]])
        .execute().actionGet()
//.setQuery(QueryBuilders.termQuery("user", "kimchy"))

assert searchResponse.failedShards == 0
assert searchResponse.hits.totalHits == 1
println "search kimchy -> hit:   ${searchResponse.hits[0].source}"




SearchRequest searchRequest = Requests.searchRequest(idx_test)
        .source(SearchSourceBuilder.searchSource().query
        (
                QueryBuilders.boolQuery()
                        .must(QueryBuilders.matchQuery('postDate', '2013-03-12'))
                        .must(QueryBuilders.nestedQuery('complex', QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("complex.detail.color", "red"))))
        )

)

searchResponse = client.search(searchRequest).actionGet()

assert searchResponse.failedShards == 0
assert searchResponse.hits.totalHits == 1
searchResponse.hits.eachWithIndex { SearchHit searchHit, Integer index ->
    println "search color red -> hit $index (score $searchHit.score): $searchHit.source"
}

// on shutdown
//node.close();
