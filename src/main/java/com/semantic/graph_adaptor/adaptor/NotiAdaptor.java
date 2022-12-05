package com.semantic.graph_adaptor.adaptor;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.google.gson.JsonObject;

public class NotiAdaptor {

    String dbName = "stellio";
    String entityColl = "entityColl";
    String propertyColl = "propertyColl";
    String edgeColl = "edgeColl";

    public void notiAdaptor(JsonObject jsonObject, ArangoDB arangoDB) {

        BaseDocument document = arangoDB.db(dbName).collection(entityColl).getDocument(jsonObject.get("id").toString().replace("\"", ""), BaseDocument.class);
        String queryPastEdge = "FOR edge IN edgeColl FILTER edge._from == \"" + document.getId() + "\" FILTER edge.attributeName == \"location\" UPDATE edge._key WITH { to: " + jsonObject.get("location").getAsJsonObject().get("observedAt") + "} in edgeColl RETURN NEW";
        ArangoCursor<BaseEdgeDocument> cursor = arangoDB.db(dbName).query(queryPastEdge, null, null, BaseEdgeDocument.class);

        DocumentCreateEntity<String> result = arangoDB.db(dbName).collection(propertyColl).insertDocument(jsonObject.get("location").toString());
        System.out.println("Property Vertex created: " + result.getId());

        String queryNewEdge = "INSERT { _from: \"" + document.getId() + "\", _to: \"" + result.getId() + "\", attributeName: \"location\", attributeType: \"GeoProperty\", from: " + jsonObject.get("location").getAsJsonObject().get("observedAt") + " } INTO " + edgeColl;
        ArangoCursor<String> cursorNew = arangoDB.db(dbName).query(queryNewEdge, null, null, String.class);

    }
}
