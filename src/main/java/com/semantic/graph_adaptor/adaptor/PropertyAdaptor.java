package com.semantic.graph_adaptor.adaptor;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.entity.EdgeDefinition;
import com.arangodb.model.DocumentCreateOptions;
import com.arangodb.model.EdgeCreateOptions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Iterator;
import java.util.Map;

public class PropertyAdaptor {

    String dbName = "stellio";
    String propertyColl = "propertyColl";
    final String edgeColl = "edgeColl";
    final String graph = "NGSI-LD_graph";

    public void storeProperty(JsonObject body, ArangoDB arangoDB) {

        System.out.println("property: "+body);
        Iterator<Map.Entry<String, JsonElement>> iterator = body.entrySet().iterator();
        Iterator<String> iteratorKey = body.keySet().iterator();

        while (iterator.hasNext()) {

            JsonElement attribute = iterator.next().getValue();
            String attName = iteratorKey.next();

            if (attribute.isJsonObject() == true) {

                if (attribute.getAsJsonObject().get("type").toString().equals("\"Property\"")) {
                    BaseDocument document = new BaseDocument();
                    document.addAttribute("value", attribute.getAsJsonObject().get("value").toString().replace("\"", ""));

                    if(attribute.getAsJsonObject().has("observedAt")) {
                        document.addAttribute("observedAt", attribute.getAsJsonObject().get("observedAt").toString().replace("\"", ""));
                    }

                    try {
                        BaseDocument result = arangoDB.db(dbName).collection(propertyColl).insertDocument(document, new DocumentCreateOptions().returnNew(true)).getNew();
                        System.out.println("Property Vertex created: " + result.getId());
                        System.out.println(result.getProperties());
                        String query = "INSERT { _from: " + body.get("_from") + ", _to: \"" + result.getId() + "\", attributeName: \"" + attName + "\", attributeType: " + attribute.getAsJsonObject().get("type") + " } INTO " + edgeColl ;
                        ArangoCursor<String> cursor = arangoDB.db(dbName).query(query, null, null, String.class);
                    } catch (ArangoDBException e) {
                        System.out.println("Failed to create property vertex. " + e.getMessage());
                    }
                } else if (attribute.getAsJsonObject().get("type").toString().equals("\"GeoProperty\"")) {
                    DocumentCreateEntity<String> result = arangoDB.db(dbName).collection(propertyColl).insertDocument(attribute.getAsJsonObject().toString());
                    System.out.println("Property Vertex created: " + result.getId());
                    String query = "INSERT { _from: " + body.get("_from") + ", _to: \"" + result.getId() + "\", attributeName: \"" + attName + "\", attributeType: " + attribute.getAsJsonObject().get("type") + " } INTO " + edgeColl ;
                    ArangoCursor<String> cursor = arangoDB.db(dbName).query(query, null, null, String.class);
                }
            }
        }
    }
}
