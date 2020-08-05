package com.example.elasticsearch;

import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.AnalyzeRequest;
import org.elasticsearch.client.indices.AnalyzeResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

@SpringBootApplication
public class ElasticsearchApplication {

    public static Boolean deleteIndex(String indexName) {
        Logger log = Logger.getGlobal();
        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("117.17.196.61", 9200, "http")));

        boolean acknowledged = false;
        try {
            // 인덱스 삭제 요청 객체
            DeleteIndexRequest request = new DeleteIndexRequest(indexName);
            AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
            acknowledged = response.isAcknowledged();
        } catch (ElasticsearchException | IOException e) {
            log.info((Supplier<String>) e);
        }
        if (acknowledged)
            log.info(indexName + " 인덱스가 삭제되었습니다.");
        else
            log.info(indexName + " 인덱스 삭제를 실패했습니다.");
        return acknowledged;
    }


    public static Boolean insertIndex(String INDEX_NAME, String TYPE_NAME) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("117.17.196.61", 9200, "http")));

        XContentBuilder indexBuilder = jsonBuilder()
                .startObject()
                .startObject(TYPE_NAME)
                .startObject("properties")
                .startObject("contents")
                .field("type", "text")
                .field("index_options", "docs")
                .endObject()
                .startObject("title")
                .field("type", "text")
                .field("index_options", "docs")
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        CreateIndexRequest request = new CreateIndexRequest(INDEX_NAME);
        request.mapping(TYPE_NAME, indexBuilder);

        // Alias 설정
        String ALIAS_NAME = "springTest_auto_alias";
        request.alias(new Alias(ALIAS_NAME));

        AcknowledgedResponse createIndexResponse =
                client.indices().create(request, RequestOptions.DEFAULT);

        boolean acknowledged = createIndexResponse.isAcknowledged();

        client.close();

        return acknowledged;
    }

    public static boolean SearchDocs(String INDEX_NAME, String TYPE_NAME, RestHighLevelClient client) throws IOException {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        String FIELD_NAME = "contents";

        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.from(0);
        searchSourceBuilder.size(5);
        //searchSourceBuilder.sort(new FieldSortBuilder(FIELD_NAME).order(SortOrder.DESC));

        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        searchRequest.types(TYPE_NAME);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);

        SearchHits searchHits = searchResponse.getHits();
        for (SearchHit hit : searchHits) {
            System.out.println(hit.toString());
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
        }
        return true;
    }

    public static boolean tokenizer(String INDEX_NAME, String TYPE_NAME, RestHighLevelClient client) throws IOException {
        AnalyzeRequest request = AnalyzeRequest.buildCustomAnalyzer("ngram")
                .build("동해물과 백두산이");


        AnalyzeResponse response = client.indices().analyze(request,RequestOptions.DEFAULT);

        List<AnalyzeResponse.AnalyzeToken> tokens = response.getTokens();

        System.out.println(tokens.get(0).getTerm());
        return true;
    }


    public static void main(String[] args) throws IOException {

        RestHighLevelClient client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("117.17.196.61", 9200, "http")));

        String INDEX_NAME = "x_test";

        String TYPE_NAME = "docs";


        //DeleteIndexRequest request = new DeleteIndexRequest(INDEX_NAME);

        //deleteIndex(INDEX_NAME);

        //insertIndex(INDEX_NAME,TYPE_NAME);

        //SearchDocs(INDEX_NAME,TYPE_NAME,client);

        tokenizer(INDEX_NAME,TYPE_NAME,client);

        //SpringApplication.run(ElasticsearchApplication.class, args);
    }

}

