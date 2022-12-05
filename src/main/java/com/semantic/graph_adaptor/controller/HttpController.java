package com.semantic.graph_adaptor.controller;

import com.arangodb.ArangoDB;
import com.google.gson.*;
import com.semantic.graph_adaptor.adaptor.NodeSeparator;
import com.semantic.graph_adaptor.adaptor.NotiAdaptor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.client.RestTemplate;

import java.io.FileNotFoundException;
import java.io.FileReader;

@Controller
public class HttpController {

    RestTemplate restTemplate = new RestTemplate();
    String brokerUrl = "https://stellio-dev.eglobalmark.com/ngsi-ld/v1/";
    String brokerUrl_Beekeeper = "entities?type=Beekeeper";
    String brokerUrl_BeeHive = "entities?type=BeeHive";
//    String brokerUrl_Sensor = "/urn:ngsi-ld:Sensor:02_jieunTest";
    String brokerUrl_Apiary = "/entities?type=Apiary";

    HttpHeaders headers = new HttpHeaders();
    NodeSeparator adaptor = new NodeSeparator();
    NotiAdaptor notiAdaptor = new NotiAdaptor();

    public void getEntities(ArangoDB arangoDB) {

        headers.set("accept", "application/ld+json");
        headers.set("Link", "https://raw.githubusercontent.com/easy-global-market/ngsild-api-data-models/master/apic/jsonld-contexts/apic-compound.jsonld");
        headers.set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwSzZPOXNBQnRuNGRaTkxScUlPN0t6RXVvUDVjWWh0TDI3clVYVURMZlRBIn0.eyJleHAiOjE2NzAzMTg2OTgsImlhdCI6MTY3MDIzMjI5OCwianRpIjoiZTFhYTYyYTktNDUyZS00ZGFiLThkNDEtNWNhNzlmYzM3MGY5IiwiaXNzIjoiaHR0cHM6Ly9zc28uZWdsb2JhbG1hcmsuY29tL2F1dGgvcmVhbG1zL3N0ZWxsaW8tZGV2IiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImI4MDg0MjU3LTE5YmMtNDA3OC1hYjZlLWQ1MzU3OTU3ZGI2MiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImtldGktSmlldW4iLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInN0ZWxsaW8tY3JlYXRvciIsIm9mZmxpbmVfYWNjZXNzIiwiZGVmYXVsdC1yb2xlcy1zdGVsbGlvLWRldiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsia2V0aS1KaWV1biI6eyJyb2xlcyI6WyJ1bWFfcHJvdGVjdGlvbiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTAuNS4xLjEiLCJjbGllbnRJZCI6ImtldGktSmlldW4iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQta2V0aS1qaWV1biIsImNsaWVudEFkZHJlc3MiOiIxMC41LjEuMSJ9.VnnZFKWm22yt1SxRFb5OfePlgNxqXtx_RjN8HAO8RnDul-8sRZM94CjrdTtaoJsYOmZvNdbNU9x85o1B2dOvT4sXwYIfwmrfz11FIIw_x56P4ytN5mOMwkVOxrqtbXp8Z9g-eK9iPeUTqNI7jSIXJZbFHrb7wbHgJuFLMWxeKi9harhUXfMV0uuQnD2Emr70weXd098eER4CoA1Jr_BXq6fJfRlugkpkkqnB8qd_UHK1af5z7iQiK0fbelV4zdZa6DZHA8EecRNmQ0a9LbLQVYtCkf7lWpaSiPyMUHbMikgGsceuqoAQY87oimbD--58wt6XTbPrBkkoIusp-JFiHw");
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

        String result_apiary = restTemplate.exchange(brokerUrl+brokerUrl_Apiary, HttpMethod.GET, entity, String.class).getBody();

        JsonParser parser_apiary = new JsonParser();
        JsonElement jsonElement_apiary = parser_apiary.parse(result_apiary);
        JsonArray jsonArray_apiary = jsonElement_apiary.getAsJsonArray();

        for(int i = 0; i < jsonArray_apiary.size(); i++) {
            JsonObject jsonObject = jsonArray_apiary.get(i).getAsJsonObject();
            adaptor.nodeSeparator(jsonObject, arangoDB);
        }

    }

    public void getVesselEntities(ArangoDB arangoDB) {
        headers.set("Accept", "application/json");
        headers.set("Content-Type", "application/ld+json");

        String vesselFile = "/src/main/java/com/semantic/graph_adaptor/controller/VesselEntityList.json";

        FileReader reader = null;

        try {
            reader = new FileReader(System.getProperty("user.dir") + vesselFile);
        } catch (FileNotFoundException e) {

        }

        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser.parse(reader);
        JsonArray jsonArray = jsonElement.getAsJsonArray();

        for (int i = 0; i<jsonArray.size(); i++) {
            System.out.println(jsonArray.get(i));
            JsonObject jsonObject = jsonArray.get(i).getAsJsonObject();
            adaptor.nodeSeparator(jsonObject, arangoDB);
        }
    }

    public void notificationHandle(ArangoDB arangoDB) {

        String notificationFile = "/src/main/java/com/semantic/graph_adaptor/controller/NotificationList.json";

        FileReader reader = null;

        try {
            reader = new FileReader(System.getProperty("user.dir") + notificationFile);
        } catch (FileNotFoundException e) {

        }

        JsonParser jsonParser = new JsonParser();
        JsonElement jsonElement = jsonParser.parse(reader);
        notiAdaptor.notiAdaptor(jsonElement.getAsJsonObject(), arangoDB);
    }

    public void createSubscription() {

        headers.set("Accept", "application/json");
        headers.set("Content-Type", "application/ld+json");
        headers.set("Authorization", "Bearer eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJwSzZPOXNBQnRuNGRaTkxScUlPN0t6RXVvUDVjWWh0TDI3clVYVURMZlRBIn0.eyJleHAiOjE2NzAzMTg2OTgsImlhdCI6MTY3MDIzMjI5OCwianRpIjoiZTFhYTYyYTktNDUyZS00ZGFiLThkNDEtNWNhNzlmYzM3MGY5IiwiaXNzIjoiaHR0cHM6Ly9zc28uZWdsb2JhbG1hcmsuY29tL2F1dGgvcmVhbG1zL3N0ZWxsaW8tZGV2IiwiYXVkIjoiYWNjb3VudCIsInN1YiI6ImI4MDg0MjU3LTE5YmMtNDA3OC1hYjZlLWQ1MzU3OTU3ZGI2MiIsInR5cCI6IkJlYXJlciIsImF6cCI6ImtldGktSmlldW4iLCJhY3IiOiIxIiwicmVhbG1fYWNjZXNzIjp7InJvbGVzIjpbInN0ZWxsaW8tY3JlYXRvciIsIm9mZmxpbmVfYWNjZXNzIiwiZGVmYXVsdC1yb2xlcy1zdGVsbGlvLWRldiIsInVtYV9hdXRob3JpemF0aW9uIl19LCJyZXNvdXJjZV9hY2Nlc3MiOnsia2V0aS1KaWV1biI6eyJyb2xlcyI6WyJ1bWFfcHJvdGVjdGlvbiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsIm1hbmFnZS1hY2NvdW50LWxpbmtzIiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJjbGllbnRIb3N0IjoiMTAuNS4xLjEiLCJjbGllbnRJZCI6ImtldGktSmlldW4iLCJwcmVmZXJyZWRfdXNlcm5hbWUiOiJzZXJ2aWNlLWFjY291bnQta2V0aS1qaWV1biIsImNsaWVudEFkZHJlc3MiOiIxMC41LjEuMSJ9.VnnZFKWm22yt1SxRFb5OfePlgNxqXtx_RjN8HAO8RnDul-8sRZM94CjrdTtaoJsYOmZvNdbNU9x85o1B2dOvT4sXwYIfwmrfz11FIIw_x56P4ytN5mOMwkVOxrqtbXp8Z9g-eK9iPeUTqNI7jSIXJZbFHrb7wbHgJuFLMWxeKi9harhUXfMV0uuQnD2Emr70weXd098eER4CoA1Jr_BXq6fJfRlugkpkkqnB8qd_UHK1af5z7iQiK0fbelV4zdZa6DZHA8EecRNmQ0a9LbLQVYtCkf7lWpaSiPyMUHbMikgGsceuqoAQY87oimbD--58wt6XTbPrBkkoIusp-JFiHw");

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
            try {
                HttpEntity<String> entity = new HttpEntity<String>(jsonArray.get(i).toString(), headers);
                String result = restTemplate.exchange(brokerUrl+"subscriptions", HttpMethod.POST, entity, String.class).getBody();
                System.out.println("Subscription created: "+result);
            } catch (Exception e) {
                System.out.println("error: " +e);
            }
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
