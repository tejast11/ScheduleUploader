package com.tejast11.UpdateHDNuts900Service;

import java.util.List;
import java.util.Objects;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class MeasurandIDTracker {
    private static final Logger log = LoggerFactory.getLogger(MeasurandIDTracker.class);
    public static void track169(Document esDoc , Document hdDoc , MongoCollection<Document> hdNuts900){
        final Object targetId = 169;
        List<Document> esMeasures = (List<Document>) esDoc.get("MeasurandData");
        List<Document> hdMeasures = (List<Document>) hdDoc.get("MeasurandData");

        //Edge Case
        if(esMeasures == null || hdMeasures == null) return;

        Document es169 = esMeasures.stream()
                        .filter(d -> targetId.equals(d.get("MeasurandId")))
                        .findFirst()
                        .orElse(null);
        Document hd169 = hdMeasures.stream()
                        .filter(d -> targetId.equals(d.get("MeasurandId")))
                        .findFirst()
                        .orElse(null);
        
        if(es169 != null && hd169 != null){
            Object newVal = es169.get("MeasurandValue");
            Object oldVal = hd169.get("MeasurandValue");

            if(!Objects.equals(newVal,oldVal)){
                hd169.put("MeasurandValue",newVal);
                hdNuts900.updateOne(
                    Filters.eq("_id", hdDoc.getObjectId("_id")),
                        new Document("$set", new Document("MeasurandData", hdMeasures))
                );
                log.info("For MeasurandId=169 MeasurandValue updated for TerminalId={}, TimeStamp:{}",esDoc.get("TerminalId"),esDoc.get("TimeStamp"));
            }
        }
    }   
}
