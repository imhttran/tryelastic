package com.tom.tryelastic;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import static org.elasticsearch.common.xcontent.XContentFactory.*;


import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@RunWith(SpringRunner.class)
@SpringBootTest
public class JsonBuilderTest {

    // instance a json mapper
    ObjectMapper mapper = new ObjectMapper(); // create once, reuse

//    RestClient restClient = RestClient.builder(
//            new HttpHost("localhost", 9200, "http"),
//            new HttpHost("localhost", 9201, "http")).build();

    RestClient restClient = RestClient.builder(
            new HttpHost("localhost", 9200, "http"))
            .build();

    Sniffer sniffer = Sniffer.builder(restClient).build();

    @Test
    public void testParseJson() {
        // note you must encode the date to the data format
        String json = "{" +
                "\"user\":\"kimchy\"," +
                "\"postDate\":\"2013-01-30\"," +
                "\"message\":\"trying out Elasticsearch\"" +
                "}";



    }

    @Test
    public void testSerializeBean() throws Exception {

        UserMessage userMessage = new UserMessage();
        userMessage.setUser("kimchy");
        userMessage.setMessage("rying out Elasticsearch");
        userMessage.setPostDate(new Date());

        // generate json
        byte[] json = mapper.writeValueAsBytes(userMessage);

    }

    @Test
    public void testHelper() throws Exception {

        // User ElasticSearch JSON helper

        XContentBuilder builder =  jsonBuilder()
                .startObject()
                .field("user", "kimchy")
                .field("postDate", new Date())
                .field("message", "trying out Elasticsearch")
                .endObject();

        System.out.println(builder.toString());
    }

    @Test
    public void testRestClientSniffer(){

    }

    @Test
    public void testRestClient() throws Exception {

        // User ElasticSearch JSON helper

        HttpEntity entity = new NStringEntity(
                "{\n" +
                        "    \"company\" : \"qbox\",\n" +
                        "    \"title\" : \"Elasticsearch rest client\"\n" +
                        "}", ContentType.APPLICATION_JSON);
        Response indexResponse = restClient.performRequest(
                "PUT",
                "/blog/post/1",
                Collections.<String, String>emptyMap(),
                entity);

        System.out.println(EntityUtils.toString(indexResponse.getEntity()));

        // Search using query parameter

        Map<String, String> paramMap = new HashMap<String, String>();
        paramMap.put("q", "company:qbox");
        paramMap.put("pretty", "true");

        Response response = restClient.performRequest("GET", "/blog/_search",
                paramMap);
        System.out.println(EntityUtils.toString(response.getEntity()));
        System.out.println("Host -" + response.getHost() );
        System.out.println("RequestLine -"+ response.getRequestLine() );


        // Search using DSL
        HttpEntity entity1 = new NStringEntity(
                "{\n" +
                        "    \"query\" : {\n" +
                        "    \"match\": { \"company\":\"qbox\"} \n" +
                        "} \n"+
                        "}", ContentType.APPLICATION_JSON);

        Response response2 = restClient.performRequest("GET", "/blog/_search",Collections.singletonMap("pretty", "true"),
                entity1);
        System.out.println(EntityUtils.toString(response2.getEntity()));


    }



    @Test
    public void testRestClientAsync() throws Exception {
        HttpEntity entity1 = new NStringEntity(
                "{\n" +
                        " \"company\" : \"qbox\",\n" +
                        " \"title\" : \"Elasticsearch rest client\"\n" +
                        "}", ContentType.APPLICATION_JSON);
        HttpEntity entity2 = new NStringEntity(
                "{\n" +
                        " \"company\" : \"supergaint\",\n" +
                        " \"title\" : \"supergaint is awesome\"\n" +
                        "}", ContentType.APPLICATION_JSON);
        HttpEntity entity3 = new NStringEntity(
                "{\n" +
                        " \"company\" : \"linux foundation\",\n" +
                        " \"title\" : \"join linux foundation\"\n" +
                        "}", ContentType.APPLICATION_JSON);
        HttpEntity[] entityArray= {entity1, entity2,entity3};
        int numRequests = 3;
        final CountDownLatch latch = new CountDownLatch(numRequests);
        for (int i = 0; i < numRequests; i++) {
            restClient.performRequestAsync(
                    "PUT",
                    "/blog/posts/" + i,
                    Collections.<String, String>emptyMap(),
                    entityArray[i],
                    new ResponseListener() {
                        @Override
                        public void onSuccess(Response response) {
                            System.out.println(response);
                            latch.countDown();
                        }
                        @Override
                        public void onFailure(Exception exception) {
                            System.out.println(exception.getMessage());
                            latch.countDown();
                        }
                    }
            );
        }
//wait for completion of all requests
        latch.await();
    }



    @Test
    public void testKeyPairMap() {
        Map<String, Object> json = new HashMap<>();
        json.put("user","kimchy");
        json.put("postDate",new Date());
        json.put("message","trying out Elasticsearch");

    }
}
