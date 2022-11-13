package com.btk.elasticsearch;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class ElasticSearch {
    public static void main(String[] args) throws IOException {
        Settings settings = Settings.builder()
                .put("cluster.name", "elasticsearch-cluster").build();

        // Transporter is deprecated. Use Rest High Level API

        TransportClient client = new PreBuiltTransportClient(settings)
                .addTransportAddress(new TransportAddress(InetAddress.getByName("localhost"), 9300));

        List<DiscoveryNode> nodeList = client.listedNodes();

        nodeList.forEach(System.out::println);

        //IndexResponse response = indexAPI(client, "nabukatnazar ", "4 attempt");

        //System.out.printf(response.toString());

        long deleted = deleteWithoutID(client, "twitter", "user", "nabukatnazar");

        System.out.println(deleted);

        SearchResponse searchReponse = client.prepareSearch("twitter")
                .setTypes("_doc")
                .setQuery(QueryBuilders.matchQuery("user", "nabukatnazar"))
                .get();

        SearchHit[] hits = searchReponse.getHits().getHits();

        Arrays.stream(hits).forEach(hit -> {
            hit.getSourceAsMap().forEach((k, v) -> System.out.println((k + " : " + v)));
        });

        /*
        GetResponse getResponse = client.prepareGet("twitter", "_doc", "1").get();

        assertNotNull(getResponse);

        Map<String, Object> clientSource = getResponse.getSource();

        clientSource.forEach((k, v) -> System.out.println((k + " : " + v)));

         */

    }

    protected static IndexResponse indexAPI(TransportClient client, String user, String message) throws IOException {
        XContentBuilder builder = jsonBuilder()
                .startObject()
                .field("user", user)
                .field("postDate", new Date())
                .field("message", message)
                .endObject();
        return client.prepareIndex("twitter", "_doc", "3")
                .setSource(builder)
                .get();
    }

    private static String deleteById(TransportClient client, String index, String type, String id){
        return client.prepareGet(index, type, id).get().getId();
    }

    private static long deleteWithoutID(TransportClient client, String index, String byField, String byFilterText){
        BulkByScrollResponse response = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
                .filter(QueryBuilders.matchQuery(byField, byFilterText))
                .source(index)
                .get();
        return (response.getDeleted());
    }
}
