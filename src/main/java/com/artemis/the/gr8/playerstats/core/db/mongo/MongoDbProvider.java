package com.artemis.the.gr8.playerstats.core.db.mongo;

import com.artemis.the.gr8.playerstats.core.db.DatabaseConfig;
import com.artemis.the.gr8.playerstats.core.db.DbProvider;
import com.artemis.the.gr8.playerstats.core.db.StatKeyUtil;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.UpdateOptions;
import java.util.List;
import org.bson.Document;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public final class MongoDbProvider implements DbProvider {

    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> playerCol;
    private MongoCollection<Document> topCol;

    @Override
    public void init(DatabaseConfig config) {
        ConnectionString cs = new ConnectionString(config.mongoUri());
        // Security validations
        try {
            List<String> hosts = cs.getHosts();
            boolean remote = false;
            for (String h : hosts) {
                String hostOnly = h == null ? "" : (h.contains(":") ? h.substring(0, h.indexOf(':')) : h);
                if (!"localhost".equalsIgnoreCase(hostOnly) && !"127.0.0.1".equals(hostOnly)) { remote = true; break; }
            }
            String uri = config.mongoUri();
            boolean hasAuth = uri != null && uri.contains("@");
            boolean tlsOn = uri != null && (uri.contains("tls=true") || uri.contains("ssl=true"));
            if (remote && !hasAuth) {
                MyLogger.logWarning("MongoDB URI appears remote and uses no credentials. Consider adding auth in production.");
            }
            if (remote && !tlsOn) {
                MyLogger.logWarning("MongoDB URI appears remote and TLS/SSL is disabled. Consider enabling TLS in production.");
            }
        } catch (Exception e) {
            MyLogger.logWarning("Failed to parse MongoDB URI for TLS detection: " + e.getMessage());
        }
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(cs)
                .build();
        client = MongoClients.create(settings);
        database = client.getDatabase(config.mongoDatabase());
        playerCol = database.getCollection(config.mongoPlayerCollection());
        topCol = database.getCollection(config.mongoTopCollection());
        if (config.verboseLogging()) {
            MyLogger.logLowLevelMsg("MongoDbProvider initialized for db='" + config.mongoDatabase() + "'");
        }
    }

    @Override
    public void start() {
        // Ensure indexes exist for efficient lookups and upserts
        try {
            if (playerCol != null) {
                playerCol.createIndex(new Document("uuid", 1), new IndexOptions().unique(true));
            }
        } catch (Exception e) {
            MyLogger.logWarning("Mongo index creation failed for player uuid: " + e.getMessage());
        }
        try {
            if (topCol != null) {
                topCol.createIndex(new Document("statKey", 1), new IndexOptions().unique(true));
            }
        } catch (Exception e) {
            MyLogger.logWarning("Mongo index creation failed for top statKey: " + e.getMessage());
        }
    }

    @Override
    public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) {
        if (client == null) return;
        if (uuid == null) return;
        if (!StatKeyUtil.isValidTrackedFormat(statKey) || statKey.length() > 128) return;
        String safeName = sanitizePlayerName(playerName);
        int safeValue = Math.max(0, value);
        Document filter = new Document("uuid", uuid.toString());
        long now = Instant.now().toEpochMilli();
        Document update = new Document("$set", new Document()
                .append("name", safeName)
                .append("updatedAt", now)
                .append("stats." + statKey, safeValue)
        );
        playerCol.updateOne(filter, update, new UpdateOptions().upsert(true));
    }

    @Override
    public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize) {
        if (client == null) return;
        if (!StatKeyUtil.isValidTrackedFormat(statKey) || statKey.length() > 128) return;
        int safeTopSize = Math.max(1, Math.min(1000, topSize));
        Document doc = new Document("statKey", statKey)
                .append("topSize", safeTopSize)
                .append("updatedAt", Instant.now().toEpochMilli());
        // build entries as an array preserving order
        var arr = new org.bson.types.BasicBSONList();
        for (Map.Entry<String, Integer> e : top.entrySet()) {
            String safe = sanitizePlayerName(e.getKey());
            int val = Math.max(0, e.getValue() == null ? 0 : e.getValue());
            arr.add(new Document("name", safe).append("value", val));
            if (arr.size() >= safeTopSize) break;
        }
        doc.append("entries", arr);
        topCol.replaceOne(Filters.eq("statKey", statKey), doc, new ReplaceOptions().upsert(true));
    }

    @Override
    public void close() {
        if (client != null) {
            try { 
                client.close(); 
            } catch (Exception e) {
                MyLogger.logWarning("Failed to close MongoDB client: " + e.getMessage());
            }
            client = null;
        }
    }

    // Helpers
    private static String sanitizePlayerName(String s) {
        if (s == null) return "";
        String cleaned = s.replaceAll("[\\p{Cntrl}]", "").trim();
        if (cleaned.length() > 32) cleaned = cleaned.substring(0, 32);
        return cleaned;
    }
}
