package com.semantic.graph_adaptor.adaptor;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.DocumentCreateEntity;
import com.arangodb.model.DocumentCreateOptions;
import com.google.gson.JsonObject;

import java.util.Iterator;

public class NotiAdaptor {

    String dbName = "stellio";
    String entityColl = "entityColl";
    String propertyColl = "propertyColl";
    String edgeColl = "edgeColl";

    public void notiAdaptor(JsonObject jsonObject, ArangoDB arangoDB) {

        if (jsonObject.has("location")) {
            BaseDocument document = arangoDB.db(dbName).collection(entityColl).getDocument(jsonObject.get("id").toString().replace("\"", ""), BaseDocument.class);
            String queryPastEdge = "FOR edge IN edgeColl FILTER edge._from == \"" + document.getId() + "\" FILTER edge.attributeName == \"location\" UPDATE edge._key WITH { to: " + jsonObject.get("location").getAsJsonObject().get("observedAt") + "} in edgeColl RETURN NEW";
            ArangoCursor<BaseEdgeDocument> cursor = arangoDB.db(dbName).query(queryPastEdge, null, null, BaseEdgeDocument.class);

            DocumentCreateEntity<String> result = arangoDB.db(dbName).collection(propertyColl).insertDocument(jsonObject.get("location").toString());
            System.out.println("Property Vertex created: " + result.getId());

            String queryNewEdge = "INSERT { _from: \"" + document.getId() + "\", _to: \"" + result.getId() + "\", attributeName: \"location\", attributeType: \"GeoProperty\", from: " + jsonObject.get("location").getAsJsonObject().get("observedAt") + " } INTO " + edgeColl;
            ArangoCursor<String> cursorNew = arangoDB.db(dbName).query(queryNewEdge, null, null, String.class);
        } else {
            BaseDocument docFrom = arangoDB.db(dbName).collection(entityColl).getDocument(jsonObject.get("id").toString().replace("\"", ""), BaseDocument.class);
            BaseDocument docTo = arangoDB.db(dbName).collection(entityColl).getDocument(jsonObject.get("destination").getAsJsonObject().get("object").toString().replace("\"", ""), BaseDocument.class);
            String queryPastEdge = "FOR edge IN edgeColl FILTER edge._from == \"" + docFrom.getId() + "\" FILTER edge.attributeName == \"destination\" UPDATE edge._key WITH { to: " + jsonObject.get("destination").getAsJsonObject().get("observedAt") + "} in edgeColl RETURN NEW";
            ArangoCursor<BaseEdgeDocument> cursor = arangoDB.db(dbName).query(queryPastEdge, null, null, BaseEdgeDocument.class);

            BaseEdgeDocument document = new BaseEdgeDocument();
            document.setFrom(docFrom.getId());
            document.setTo(docTo.getId());
            document.addAttribute("attributeName", "destination");
            document.addAttribute("observedAt", jsonObject.get("destination").getAsJsonObject().get("observedAt").toString().replace("\"", ""));
            document.addAttribute("from", jsonObject.get("destination").getAsJsonObject().get("observedAt").toString().replace("\"", ""));

            BaseEdgeDocument result = arangoDB.db(dbName).collection(edgeColl).insertDocument(document, new DocumentCreateOptions().returnNew(true)).getNew();

        }



    }
}
