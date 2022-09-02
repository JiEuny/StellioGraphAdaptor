package com.semantic.graph_adaptor.controller;

import com.arangodb.ArangoDB;
import com.google.gson.*;
import com.semantic.graph_adaptor.adaptor.NodeSeparator;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestTemplate;

import javax.swing.text.Style;
import java.io.FileNotFoundException;
import java.io.FileReader;

@Controller
public class HttpController {

    RestTemplate restTemplate = new RestTemplate();
    String brokerUrl = "https://stellio-dev.eglobalmark.com/ngsi-ld/v1";
    String brokerUrl_Beekeeper = "/entities?type=Beekeeper";
    String brokerUrl_BeeHive = "/entities?type=BeeHive";
//    String brokerUrl_Sensor = "/entities?type=Sensor";
//    String brokerUrl_Sensor = "/entities/urn:ngsi-ld:Sensor:02_jieunTest";
    String brokerUrl_Apiary = "/entities?type=Apiary";
    HttpHeaders headers = new HttpHeaders();
    NodeSeparator adaptor = new NodeSeparator();

    public void getEntities(ArangoDB arangoDB) {

        headers.set("accept", "application/ld+json");
        headers.set("Link", "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld");
        headers.set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwSzZPOXNBQnRuNGRaTkxScUlPN0t6RXVvUDVjWWh0TDI3clVYVURMZlRBIn0.eyJleHAiOjE2NjIxOTQ4MDQsImlhdCI6MTY2MjEwODQwNCwianRpIjoiMzRjNzJkN2UtN2FmMi00MThhLTkxMTgtZmM3ZGFmNzExNWFjIiwiaXNzIjoiaHR0cHM6Ly9zc28uZWdsb2JhbG1hcmsuY29tL2F1dGgvcmVhbG1zL3N0ZWxsaW8tZGV2IiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImI4MDg0MjU3LTE5YmMtNDA3OC1hYjZlLWQ1MzU3OTU3ZGI2MiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImtldGktSmlldW4iLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInN0ZWxsaW8tY3JlYXRvciIsIm9mZmxpbmVfYWNjZXNzIiwiZGVmYXVsdC1yb2xlcy1zdGVsbGlvLWRldiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsia2V0aS1KaWV1biI6eyJyb2xlcyI6WyJ1bWFfcHJvdGVjdGlvbiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTgzLjEwNi4xNjkuMTEzIiwiY2xpZW50SWQiOiJrZXRpLUppZXVuIiwicHJlZmVycmVkX3VzZXJuYW1lIjoic2VydmljZS1hY2NvdW50LWtldGktamlldW4iLCJjbGllbnRBZGRyZXNzIjoiMTgzLjEwNi4xNjkuMTEzIn0.XI0JMbtFGoBp650iIvOuBE0AlKpnqGmSKzrK9VSJFO303yyMrSmWhAHEquv4FgxhhYLzGlAzQjH7l436rzOnNSJoUasiAVkTDXkj0ww7gzy7lpI8yP074zngzTwc2LYnzi9u3nQXTwifPsuafLDuCPnoE3lUFYGMGPEfp1z6liKxMkKOFGGs957zTJ6ch2Hh8sx4UIyU5kgOKSZWZcGMB2QbhP9gQrqeqx_azlbLGu3zrKRFd4xuwjFdgBj6bKbuZr3Wj65xNIE94tShGwo1BG81WTHpNZUnzMJj21xOBHeXP6xfF1aFnKs5ZBSgqnJ1QLD3X0v1wvUIoyk6JWTqUg");
        HttpEntity<String> entity = new HttpEntity<>(headers);

        String result_beek = restTemplate.exchange(brokerUrl+brokerUrl_Beekeeper, HttpMethod.GET, entity, String.class).getBody();

//        System.out.println(result_beek);

        JsonParser parser_beek = new JsonParser();
        JsonElement jsonElement_beek = parser_beek.parse(result_beek);
        JsonArray jsonArray_beek = jsonElement_beek.getAsJsonArray();

        for(int i = 0; i < jsonArray_beek.size(); i++) {
            JsonObject jsonObject = jsonArray_beek.get(i).getAsJsonObject();
            adaptor.nodeSeparator(jsonObject, arangoDB);
        }

        String result_beeHive = restTemplate.exchange(brokerUrl+brokerUrl_BeeHive, HttpMethod.GET, entity, String.class).getBody();

//        System.out.println(result_beeHive);

        JsonParser parser_beeH = new JsonParser();
        JsonElement jsonElement_beeH = parser_beeH.parse(result_beeHive);
        JsonArray jsonArray_beeH = jsonElement_beeH.getAsJsonArray();

        for(int i = 0; i < jsonArray_beeH.size(); i++) {
            JsonObject jsonObject = jsonArray_beeH.get(i).getAsJsonObject();
            adaptor.nodeSeparator(jsonObject, arangoDB);
        }

//        String result_sensor = restTemplate.exchange(brokerUrl+brokerUrl_Sensor, HttpMethod.GET, entity, String.class).getBody();

//        System.out.println(result_sensor);

//        JsonParser parser_sensor = new JsonParser();
//        JsonElement jsonElement_sensor = parser_sensor.parse(result_sensor);
//        JsonArray jsonArray_sensor = jsonElement_sensor.getAsJsonArray();
//
//        for(int i = 0; i < jsonArray_sensor.size(); i++) {
//            JsonObject jsonObject = jsonArray_sensor.get(i).getAsJsonObject();
//            adaptor.nodeSeparator(jsonObject, arangoDB);
//        }

//        JsonParser parser_sensor = new JsonParser();
////        JsonElement jsonElement_sensor = parser_sensor.parse(result_sensor);
////        JsonArray jsonArray_sensor = jsonElement_sensor.getAsJsonArray();
//
////        for(int i = 0; i < jsonArray_sensor.size(); i++) {
//            JsonObject jsonObjectSensor = parser_sensor.parse(result_sensor).getAsJsonObject();
//            adaptor.nodeSeparator(jsonObjectSensor, arangoDB);
////        }

        String result_apiary = restTemplate.exchange(brokerUrl+brokerUrl_Apiary, HttpMethod.GET, entity, String.class).getBody();

//        System.out.println(result_sensor);

        JsonParser parser_apiary = new JsonParser();
        JsonElement jsonElement_apiary = parser_apiary.parse(result_apiary);
        JsonArray jsonArray_apiary = jsonElement_apiary.getAsJsonArray();

        for(int i = 0; i < jsonArray_apiary.size(); i++) {
            JsonObject jsonObject = jsonArray_apiary.get(i).getAsJsonObject();
            adaptor.nodeSeparator(jsonObject, arangoDB);
        }

    }

    public void createSubscription() {

        headers.set("Accept", "application/json");
        headers.set("Content-Type", "application/ld+json");

        String subscriptionFile = "/src/main/java/com/semantic/graph_adaptor/controller/SubscriptionList.json";

        FileReader reader = null;

        try {
            reader = new FileReader(System.getProperty("user.dir") + subscriptionFile);
        } catch (FileNotFoundException e) {

        }

        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser.parse(reader);
        JsonArray jsonArray = jsonElement.getAsJsonArray();

        for (int i = 0; i<jsonArray.size(); i++) {
            HttpEntity<String> entity = new HttpEntity<String>(jsonArray.get(i).toString(), headers);
            String result = restTemplate.exchange(brokerUrl+"/subscriptions", HttpMethod.POST, entity, String.class).getBody();
            System.out.println("Subscription created: "+result);
        }

    }

    @RequestMapping(value = "/notify")
    public String notification(@RequestBody String requestbody) {

        JsonParser parser = new JsonParser();
        JsonElement jsonElement = parser.parse(requestbody).getAsJsonObject();
        JsonObject jsonObject = (JsonObject)jsonElement;
        JsonArray jsonArray = jsonObject.get("data").getAsJsonArray();

        System.out.println("Notification created: ");

        if(jsonObject.get("subscriptionId").toString().split(":")[3].equals("Sensor\"")) {
            System.out.println(jsonArray);

        }

        return "";
    }
}
