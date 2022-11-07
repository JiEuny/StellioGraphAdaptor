package com.semantic.graph_adaptor.adaptor;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.model.DocumentCreateOptions;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import java.util.Iterator;
import java.util.Map;

public class RelationshipAdaptor {

    String dbName = "stellio";
    String entityColl = "entityColl";
    final String edgeColl = "edgeColl";

    public void storeRelationship(JsonObject body, ArangoDB arangoDB) {

        System.out.println("relationship: "+body);

        Iterator<Map.Entry<String, JsonElement>> iterator = body.entrySet().iterator();
        Iterator<String> iteratorKey = body.keySet().iterator();

        while (iterator.hasNext()) {

            JsonElement attribute = iterator.next().getValue();
            String attName = iteratorKey.next();

            if (attribute.isJsonObject() == true) {

                if (attribute.getAsJsonObject().get("type").toString().equals("\"Relationship\"")) {
                    BaseEdgeDocument document = new BaseEdgeDocument();
                    document.setFrom(body.get("_from").toString().replaceAll("\"", ""));
                    document.setTo(entityColl + "/" + attribute.getAsJsonObject().get("object").toString().replaceAll("\"", ""));
                    document.addAttribute("attributeName", attName);

                    Iterator<String> iteratorKey1 = attribute.getAsJsonObject().keySet().iterator();
                    while (iteratorKey1.hasNext()) {
                        String key = iteratorKey1.next();

                        if(key.equals("observedAt")) {
                            document.addAttribute("observedAt", attribute.getAsJsonObject().get("observedAt").toString().replace("\"", ""));
                        } else if (key.equals("type")) {
                            System.out.println("test");
                        } else if (key.equals("object")) {
                            System.out.println("test");
                        } else {
                            document.addAttribute(key, attribute.getAsJsonObject().get(key));
                        }
                    }
                    try {
                        BaseEdgeDocument result = arangoDB.db(dbName).collection(edgeColl).insertDocument(document, new DocumentCreateOptions().returnNew(true)).getNew();
                        System.out.println("Relationship Edge created: " + result.getId());
                    } catch (ArangoDBException e) {
                        System.out.println("Failed to get document: " + attribute.getAsJsonObject().get("object") + "; " + e.getMessage());
                    }
                }

            }

        }

    }
}
