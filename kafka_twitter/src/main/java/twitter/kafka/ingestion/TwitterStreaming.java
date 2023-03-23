package twitter.kafka.ingestion;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONArray;
import org.json.JSONObject;

import twitter.kafka.common.Config;
import twitter.kafka.producer.TwitterProducer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TwitterStreaming {

    public static void main(String[] args) throws IOException, URISyntaxException {
        Config config = new Config();
        String bearerToken = config.bearerToken;

        if (bearerToken == null) {
            System.out.println("There was a problem getting your bearer token. Please make sure you set the BEARER_TOKEN environment variable");
        } else {
            Map<String, String> rules = config.map;
            System.out.println(rules);
            setupRules(bearerToken, rules);
            connectStream(bearerToken);
        }
    }

    private static void connectStream(String bearerToken) throws IOException, URISyntaxException {

        TwitterProducer tweetProducer = new TwitterProducer();

        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream");

        // GET Method 사용해서 url 호출
        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));

        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        JSONArray tweets = new JSONArray();
        Long start = System.currentTimeMillis();
        if (null != entity) {
            BufferedReader reader = new BufferedReader(new InputStreamReader((entity.getContent())));
            System.out.println("start!");
            String line = reader.readLine();
            try {
                while (line != null) {
                    if (line.length() == 0) {
                        System.out.println(line);
                        line = reader.readLine();
                    } else if (line.charAt(0) == '{') {
                        try {
                            JSONObject data = (JSONObject) new JSONObject(line).get("data");
                            System.out.println(data.get("text"));
                            //                    tweets.put(data);
                            ProducerRecord<String, String> record = new ProducerRecord<>(
                                    "twitter-chatgpt",
                                    line
                            );
                            tweetProducer.producer.send(record);
                            tweetProducer.producer.flush();

                            line = reader.readLine();
                            System.out.println("save");
                        } catch (ClassCastException e) {
                            e.printStackTrace();
                            System.out.println(new JSONObject(line));
                        }
                    } else {
                        System.out.println(line);
                        line = reader.readLine();
                    }
                }
            } catch (Exception e) {
                tweetProducer.producer.abortTransaction(); // 프로듀서 트랜잭션 중단
                e.printStackTrace();
            } finally {
                tweetProducer.producer.commitTransaction(); // 프로듀서 트랜잭션 커밋
                tweetProducer.producer.close();
            }
        }



    }

    // rule 재설정하는 method
    private static void setupRules(String bearerToken, Map<String, String> rules) throws IOException, URISyntaxException {
        List<String> existingRules = getRules(bearerToken);
        if (existingRules.size() > 0) {
            deleteRules(bearerToken, existingRules);
        }
        createRules(bearerToken, rules);
    }

    // rule 생성하는 method
    private static void createRules(String bearerToken, Map<String, String> rules) throws URISyntaxException, IOException {
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{\"add\": [%s]}", rules), ContentType.APPLICATION_JSON);
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity));
        }
    }

    // 현재 uri내 존재하는 rule 찾는 method
    private static List<String> getRules(String bearerToken) throws URISyntaxException, IOException {
        List<String> rules = new ArrayList<>();
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpGet httpGet = new HttpGet(uriBuilder.build());
        httpGet.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpGet.setHeader("content-type", "application/json");
        HttpResponse response = httpClient.execute(httpGet);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            JSONObject json = new JSONObject(EntityUtils.toString(entity, "UTF-8"));
            if (json.length() > 1) {
                JSONArray array = (JSONArray) json.get("data");
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jsonObject = (JSONObject) array.get(i);
                    rules.add(jsonObject.getString("id"));
                }
            }
        }
        return rules;
    }

    // 존재하는 rule 제거하는 method
    private static void deleteRules(String bearerToken, List<String> existingRules) throws URISyntaxException, IOException {
        HttpClient httpClient = HttpClients.custom()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setCookieSpec(CookieSpecs.STANDARD).build())
                .build();

        URIBuilder uriBuilder = new URIBuilder("https://api.twitter.com/2/tweets/search/stream/rules");

        HttpPost httpPost = new HttpPost(uriBuilder.build());
        httpPost.setHeader("Authorization", String.format("Bearer %s", bearerToken));
        httpPost.setHeader("content-type", "application/json");
        StringEntity body = new StringEntity(getFormattedString("{ \"delete\": { \"ids\": [%s]}}", existingRules));
        httpPost.setEntity(body);
        HttpResponse response = httpClient.execute(httpPost);
        HttpEntity entity = response.getEntity();
        if (null != entity) {
            System.out.println(EntityUtils.toString(entity, "UTF-8"));
        }
    }

    // id를 "id"형태로 변환해 ','로 연결한 문자열 return
    private static String getFormattedString(String string, List<String> ids) {
        StringBuilder sb = new StringBuilder();
        if (ids.size() == 1) {
            return String.format(string, "\"" + ids.get(0) + "\"");
        } else {
            for (String id : ids) {
                sb.append("\"" + id + "\"" + ",");
            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

    // rule을 "value", "tag"를 key로 가지는 json 형태로 변환하는 method
    private static String getFormattedString(String string, Map<String, String> rules) {
        StringBuilder sb = new StringBuilder();
        if (rules.size() == 1) {
            String key = rules.keySet().iterator().next();
            String tag = rules.get(key);
            if (tag == null) {
                return String.format(string, "{\"value\": \"" + key + "\"}");

            } else {
                return String.format(string, "{\"value\": \"" + key + "\", \"tag\": \"" + rules.get(key) + "\"}");

            }
        } else {
            for (Map.Entry<String, String> entry : rules.entrySet()) {
                String value = entry.getKey();
                String tag = entry.getValue();
                // tag 존재할 경우에만 tag 추가
                if (tag == null) {
                    sb.append("{\"value\": \"" + value + "\"}" + ",");
                } else {
                    sb.append("{\"value\": \"" + value + "\", \"tag\": \"" + tag + "\"}" + ",");
                }

            }
            String result = sb.toString();
            return String.format(string, result.substring(0, result.length() - 1));
        }
    }

}
