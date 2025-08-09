package com.tejast11.UpdateHDNuts900Service;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class MeasurandIDTracker {
    private static final Logger log = LoggerFactory.getLogger(MeasurandIDTracker.class);
    private static final Object TARGET_ID = 169;
    
    public static void track169(Document esDoc, Document hdDoc, MongoCollection<Document> hdNuts900) {
        @SuppressWarnings("unchecked")
        List<Document> esMeasures = (List<Document>) esDoc.get("MeasurandData");
        @SuppressWarnings("unchecked")
        List<Document> hdMeasures = (List<Document>) hdDoc.get("MeasurandData");

        // Edge Case - early return to save memory
        if (esMeasures == null || hdMeasures == null) return;

        // Find MeasurandId=169 in ES data
        Document es169 = null;
        for (Document d : esMeasures) {
            if (TARGET_ID.equals(d.get("MeasurandId"))) {
                es169 = d;
                break;
            }
        }
        
        // Find MeasurandId=169 in HD data
        Document hd169 = null;
        for (Document d : hdMeasures) {
            if (TARGET_ID.equals(d.get("MeasurandId"))) {
                hd169 = d;
                break;
            }
        }
        
        if (es169 != null && hd169 != null) {
            // Check if ANY field has changed in MeasurandId=169
            boolean hasChanges = false;
            StringBuilder changeLog = new StringBuilder();
            
            Set<String> esFields = es169.keySet();
            for (String field : esFields) {
                Object newVal = es169.get(field);
                Object oldVal = hd169.get(field);
                
                if (!Objects.equals(newVal, oldVal)) {
                    hasChanges = true;
                    changeLog.append(field).append(": ")
                             .append(oldVal).append(" → ").append(newVal).append(", ");
                }
            }
            
            // If MeasurandId=169 has changes, replace ENTIRE MeasurandData array with ES array
            if (hasChanges) {
                // Replace the entire MeasurandData array from ES
                hdNuts900.updateOne(
                    Filters.eq("_id", hdDoc.getObjectId("_id")),
                    new Document("$set", new Document("MeasurandData", esMeasures))
                );
                
                // Clean up log message
                String changes = changeLog.toString();
                if (changes.endsWith(", ")) {
                    changes = changes.substring(0, changes.length() - 2);
                }
                
                log.info("MeasurandId=169 changed - ENTIRE MeasurandData array replaced for TerminalId={}, TimeStamp={}, Changes: [{}]", 
                        esDoc.get("TerminalId"), esDoc.get("TimeStamp"), changes);
            }
        }
    }
    
    public static void trackAndReplaceArrayForMeasurandId(Document esDoc, Document hdDoc, 
                                                         MongoCollection<Document> hdNuts900, 
                                                         Object targetMeasurandId) {
        @SuppressWarnings("unchecked")
        List<Document> esMeasures = (List<Document>) esDoc.get("MeasurandData");
        @SuppressWarnings("unchecked")
        List<Document> hdMeasures = (List<Document>) hdDoc.get("MeasurandData");

        if (esMeasures == null || hdMeasures == null) return;

        Document esMeasurand = null;
        Document hdMeasurand = null;
        
        // Find the specific MeasurandId in both collections
        for (Document d : esMeasures) {
            if (targetMeasurandId.equals(d.get("MeasurandId"))) {
                esMeasurand = d;
                break;
            }
        }
        
        for (Document d : hdMeasures) {
            if (targetMeasurandId.equals(d.get("MeasurandId"))) {
                hdMeasurand = d;
                break;
            }
        }
        
        if (esMeasurand != null && hdMeasurand != null) {
            // Check for any changes across all fields
            boolean hasChanges = false;
            Set<String> esFields = esMeasurand.keySet();
            
            for (String field : esFields) {
                if (!Objects.equals(esMeasurand.get(field), hdMeasurand.get(field))) {
                    hasChanges = true;
                    break; // Exit early if any change is found
                }
            }
            
            // If target MeasurandId has changes, replace ENTIRE MeasurandData array
            if (hasChanges) {
                hdNuts900.updateOne(
                    Filters.eq("_id", hdDoc.getObjectId("_id")),
                    new Document("$set", new Document("MeasurandData", esMeasures))
                );
                
                log.info("MeasurandId={} changed - ENTIRE MeasurandData array replaced for TerminalId={}, TimeStamp={}", 
                        targetMeasurandId, esDoc.get("TerminalId"), esDoc.get("TimeStamp"));
            }
        }
    }
}