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
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;


@Service
public class UpdateService {
    private static final String MONGO_URI = "mongodb://localhost:27017";
    private static final String DATABASE_NAME = "ESDSI";
    private static final Logger log = LoggerFactory.getLogger(UpdateService.class);

    private MongoClient mongoClient;
    private static MongoDatabase database;

    public void connectDB(){
        try{
            mongoClient = MongoClients.create(MONGO_URI);
            database = mongoClient.getDatabase(DATABASE_NAME);
            System.out.println("DB connection successfull: "+ DATABASE_NAME);
        }catch(Exception e){
            log.error("Failed to connect to MongoDB", e);
            throw new RuntimeException("Failed to connect to MongoDB", e);
        }
    }
    public UpdateService(){
        connectDB();
        log.info("Updating application is running in background"); //"Background synchronization service initialized."
    }
    @Scheduled(fixedDelay = 1000)
    public void updateHDNuts900(){
        MongoCollection<Document> esSchedule = database.getCollection("ESSchedule");
        MongoCollection<Document> hdNuts900 = database.getCollection("HDNuts900");

        List<Document> esDocs = new ArrayList<>();
        esSchedule.find().into(esDocs);

        boolean logUpdate = false;
        for(Document esDoc : esDocs){
            Object terminalId = esDoc.get("TerminalId");
            Date timeStamp = esDoc.getDate("TimeStamp");
            
            //Edge-Case
            if(terminalId == null || timeStamp == null) {
                log.warn("Skipping document with missing TerminalId or TimeStamp.");
                continue;
            }

            //Make a check of both terminal-id and time-stamp
            Document hdDoc = hdNuts900.find(Filters.and(
                Filters.eq("TerminalId",terminalId),
                Filters.eq("TimeStamp",timeStamp)
            )).first();


            if(hdDoc != null){
                List<Document> esMeasures = (List<Document>) esDoc.get("MeasurandData");
                List<Document> hdMeasures = (List<Document>) hdDoc.get("MeasurandData");

                if(esMeasures == null) esMeasures = new ArrayList<>();
                if(hdMeasures == null) hdMeasures = new ArrayList<>();

                Set<Object> existingIds = hdMeasures.stream()
                .map(d -> d.get("MeasurandId"))
                .collect(Collectors.toSet());

                boolean updated = false;
                for(Document esMeasure : esMeasures){
                    Object id = esMeasure.get("MeasurandId");
                    if(!existingIds.contains(id)){
                        hdMeasures.add(esMeasure);
                        logUpdate = true;
                        updated = true;
                    }
                }
                if(updated){
                    hdNuts900.updateOne(
                        Filters.eq("_id" , hdDoc.getObjectId("_id")),
                        new Document("$set",new Document("MeasurandData",hdMeasures))
                    );
                    logUpdate = true;
                    log.info("Updated MeasurandData for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
                }else{
                    log.debug("No update needed for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
                }
                MeasurandIDTracker.track169(esDoc, hdDoc, hdNuts900); /* Tracks MasurandId(169)'s MeasurandValue,
                if the MeaurandValue is incremented for MeasuarandId(169) it will be updated instantly */
            }else{
                Document newHdDoc = new Document()
                .append("TerminalId",terminalId)
                .append("TerminalName",esDoc.get("TerminalName"))
                .append("TimeStamp" , timeStamp)
                .append("TimeStampId", esDoc.get("TimeStampId"))
                .append("MeasurandData",esDoc.get("MeasurandData",new ArrayList<Document>()));
                hdNuts900.insertOne(newHdDoc);
                log.info("Inserted new document in HDNuts900 for TerminalId: {}, TimeStamp: {}", terminalId, timeStamp);
            }
        }
        if (logUpdate) {
        log.info("Changes Updated into database");
        }
    }
}
