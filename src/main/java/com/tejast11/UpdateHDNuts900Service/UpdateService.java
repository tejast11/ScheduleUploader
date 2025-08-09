package com.tejast11.UpdateHDNuts900Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.bson.Document;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import jakarta.annotation.PreDestroy;

@Service
public class UpdateService {
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "ESDSI";
    private static final Logger log = LoggerFactory.getLogger(UpdateService.class);
    
    // Process in small batches to prevent memory overflow
    private static final int BATCH_SIZE = 50;

    private MongoClient mongoClient;
    private MongoDatabase database;
    private volatile boolean isProcessing = false;

    public void connectDB(){
        try{
            mongoClient = MongoClients.create(MONGO_URI);
            database = mongoClient.getDatabase(DATABASE_NAME);
            System.out.println("DB connection successful: "+ DATABASE_NAME);
        }catch(Exception e){
            log.error("Failed to connect to MongoDB", e);
            throw new RuntimeException("Failed to connect to MongoDB", e);
        }
    }

    public UpdateService(){
        connectDB();
        log.info("Updating application is running in background");
    }

    @Scheduled(fixedDelay = 300000)
    public void updateHDNuts900(){
        // Prevent concurrent executions to avoid memory buildup
        if(isProcessing) {
            log.debug("Previous update still processing, skipping");
            return;
        }
        
        isProcessing = true;
        try {
            processUpdatesInBatches();
        } finally {
            isProcessing = false;
        }
    }

    private void processUpdatesInBatches() {
        MongoCollection<Document> esSchedule = database.getCollection("ESSchedule");
        MongoCollection<Document> hdNuts900 = database.getCollection("HDNuts900");

        boolean logUpdate = false;
        int processedCount = 0;

        // Use cursor instead of loading all documents into memory
        try (MongoCursor<Document> cursor = esSchedule.find().iterator()) {
            List<Document> batch = new ArrayList<>();
            
            while (cursor.hasNext()) {
                batch.add(cursor.next());
                
                // Process in batches to control memory usage
                if (batch.size() >= BATCH_SIZE) {
                    boolean batchUpdated = processBatch(batch, hdNuts900);
                    if (batchUpdated) logUpdate = true;
                    
                    processedCount += batch.size();
                    batch.clear(); // Clear batch to free memory
                    
                    // Force garbage collection hint for long-running service
                    if (processedCount % (BATCH_SIZE * 10) == 0) {
                        System.gc();
                    }
                }
            }
            
            // Process remaining documents
            if (!batch.isEmpty()) {
                boolean batchUpdated = processBatch(batch, hdNuts900);
                if (batchUpdated) logUpdate = true;
                processedCount += batch.size();
                batch.clear();
            }
        }

        if (logUpdate) {
            log.info("Changes Updated into database - Processed: {} documents", processedCount);
        }
    }

    private boolean processBatch(List<Document> batch, MongoCollection<Document> hdNuts900) {
        boolean anyUpdated = false;
        
        for (Document esDoc : batch) {
            Object terminalId = esDoc.get("TerminalId");
            Date timeStamp = esDoc.getDate("TimeStamp");
            
            // Edge-Case
            if (terminalId == null || timeStamp == null) {
                log.warn("Skipping document with missing TerminalId or TimeStamp.");
                continue;
            }

            // Make a check of both terminal-id and time-stamp
            Document hdDoc = hdNuts900.find(Filters.and(
                Filters.eq("TerminalId", terminalId),
                Filters.eq("TimeStamp", timeStamp)
            )).first();

            if (hdDoc != null) {
                List<Document> esMeasures = getDocumentList(esDoc, "MeasurandData");
                List<Document> hdMeasures = getDocumentList(hdDoc, "MeasurandData");

                Set<Object> existingIds = hdMeasures.stream()
                    .map(d -> d.get("MeasurandId"))
                    .collect(Collectors.toSet());

                boolean updated = false;
                for (Document esMeasure : esMeasures) {
                    Object id = esMeasure.get("MeasurandId");
                    if (!existingIds.contains(id)) {
                        hdMeasures.add(esMeasure);
                        updated = true;
                    }
                }
                
                if (updated) {
                    hdNuts900.updateOne(
                        Filters.eq("_id", hdDoc.getObjectId("_id")),
                        new Document("$set", new Document("MeasurandData", hdMeasures))
                    );
                    anyUpdated = true;
                    log.info("Updated MeasurandData for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
                } else {
                    log.debug("No update needed for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
                }
                
                MeasurandIDTracker.track169(esDoc, hdDoc, hdNuts900);
                
            } else {
                Document newHdDoc = new Document()
                    .append("TerminalId", terminalId)
                    .append("TerminalName", esDoc.get("TerminalName"))
                    .append("TimeStamp", timeStamp)
                    .append("TimeStampId", esDoc.get("TimeStampId"))
                    .append("MeasurandData", getDocumentList(esDoc, "MeasurandData"));
                    
                hdNuts900.insertOne(newHdDoc);
                anyUpdated = true;
                log.info("Inserted new document in HDNuts900 for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
            }
        }
        
        return anyUpdated;
    }

    @SuppressWarnings("unchecked")
    private List<Document> getDocumentList(Document doc, String fieldName) {
        List<Document> list = (List<Document>) doc.get(fieldName);
        return list != null ? list : new ArrayList<>();
    }

    @PreDestroy
    public void cleanup() {
        if (mongoClient != null) {
            mongoClient.close();
            log.info("MongoDB connection closed");
        }
    }
}