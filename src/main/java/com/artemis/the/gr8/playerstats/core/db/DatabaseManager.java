package com.artemis.the.gr8.playerstats.core.db;

import com.artemis.the.gr8.playerstats.core.config.ConfigHandler;
import com.artemis.the.gr8.playerstats.core.utils.Closable;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import com.artemis.the.gr8.playerstats.core.db.mongo.MongoDbProvider;
import com.artemis.the.gr8.playerstats.core.db.postgres.PostgresProvider;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Central entry point for database operations. Selects a provider based on config and proxies calls.
 * Currently uses a No-Op provider until concrete providers are implemented.
 */
public final class DatabaseManager implements Closable {

    private static volatile DatabaseManager instance;

    public static DatabaseManager getInstance() {
        DatabaseManager local = instance;
        if (local != null) return local;
        synchronized (DatabaseManager.class) {
            if (instance == null) instance = new DatabaseManager();
            return instance;
        }
    }

    private DatabaseConfig configSnapshot;
    private List<String> trackedKeysCache;
    private DbProvider provider;

    // Async execution and simple write-dedup caches
    private ExecutorService executor;
    private static final int DEFAULT_QUEUE_CAPACITY = 5000;
    private final ConcurrentHashMap<String, PlayerStatCacheEntry> playerCache = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, TopCacheEntry> topCache = new ConcurrentHashMap<>();

    private DatabaseManager() {
        reloadFromConfig();
    }

    public void reloadFromConfig() {
        this.configSnapshot = DatabaseConfig.from(ConfigHandler.getInstance());
        // Determine tracked keys: use configured list when provided, else enumerate all
        List<String> configured = configSnapshot.trackedStats();
        java.util.ArrayList<String> keys = new java.util.ArrayList<>();
        boolean usedConfigured = configured != null && !configured.isEmpty();
        if (usedConfigured) {
            for (String s : configured) {
                if (s == null) continue;
                String k = s.trim();
                if (k.isEmpty()) continue;
                if (StatKeyUtil.isValidTrackedFormat(k) && k.length() <= 128) {
                    if (!keys.contains(k)) keys.add(k);
                } else {
                    MyLogger.logWarning("Ignoring invalid tracked-stats entry: '" + k + "'");
                }
            }
            if (keys.isEmpty()) {
                usedConfigured = false; // fallback
            }
        }
        if (!usedConfigured) {
            keys.addAll(StatKeyUtil.enumerateAllKeys());
        }
        this.trackedKeysCache = java.util.Collections.unmodifiableList(keys);
        MyLogger.logLowLevelMsg("DatabaseManager tracking " + trackedKeysCache.size() + " statistic keys (source=" + (usedConfigured ? "config" : "auto") + ")");
        dbLog("DB config: type=" + configSnapshot.type() + 
                ", asyncThreads=" + Math.max(1, configSnapshot.asyncThreads()) +
                ", playerUpdateMinIntervalMs=" + configSnapshot.playerUpdateMinIntervalMs() +
                ", topUpsertMinIntervalMs=" + configSnapshot.topUpsertMinIntervalMs() +
                ", topListSize=" + configSnapshot.topListSize() +
                ", generateTopOnLoad=" + configSnapshot.generateTopOnLoad() +
                ", generateTopPeriodically=" + configSnapshot.generateTopPeriodically() +
                ", generateTopIntervalMinutes=" + configSnapshot.generateTopIntervalMinutes() +
                ", updatePlayerOnJoin=" + configSnapshot.updatePlayerOnJoin());
        if (!configSnapshot.enabled()) {
            shutdownExecutor();
            provider = new NoopProvider();
            MyLogger.logLowLevelMsg("Database disabled in config; using No-Op provider.");
            dbLog("DB disabled -> No-Op provider active");
            return;
        }
        // Select provider based on type
        provider = switch (configSnapshot.type()) {
            case MONGO -> new MongoDbProvider();
            case POSTGRES -> new PostgresProvider();
        };
        try {
            provider.init(configSnapshot);
            provider.start();
            // (Re)initialize async executor
            initExecutor(Math.max(1, configSnapshot.asyncThreads()), DEFAULT_QUEUE_CAPACITY);
            dbLog("DB provider initialized: " + provider.getClass().getSimpleName());
        } catch (Exception e) {
            MyLogger.logWarning("Failed to initialize DB provider; falling back to No-Op. " + e.getMessage());
            provider = new NoopProvider();
            shutdownExecutor();
        }
    }

    public DatabaseConfig config() { return configSnapshot; }

    public List<String> trackedStatKeys() {
        return trackedKeysCache;
    }

    public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) {
        if (executor == null) {
            // Fallback (should not happen when DB is enabled)
            provider.updatePlayerStat(uuid, playerName, statKey, value);
            return;
        }
        long now = System.currentTimeMillis();
        String key = uuid + "|" + statKey;
        PlayerStatCacheEntry prev = playerCache.get(key);
        long minInterval = Math.max(0L, configSnapshot.playerUpdateMinIntervalMs());
        if (prev != null && prev.value == value && (now - prev.lastWriteAt) < minInterval) {
            dbLog("DB skip playerStat (unchanged within interval): " + key);
            return;
        }
        dbLog("Queue playerStat: " + key + " value=" + value);
        playerCache.put(key, new PlayerStatCacheEntry(value, now));
        executor.submit(() -> {
            try {
                provider.updatePlayerStat(uuid, playerName, statKey, value);
                dbLog("Wrote playerStat: " + key + " value=" + value);
            } catch (Exception e) {
                MyLogger.logWarning("Async updatePlayerStat failed: " + e.getMessage());
            }
        });
    }

    public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top) {
        int max = configSnapshot.topListSize();
        if (executor == null) {
            provider.upsertTopList(statKey, top, max);
            return;
        }
        long now = System.currentTimeMillis();
        String hash = hashTop(top, max);
        TopCacheEntry prev = topCache.get(statKey);
        long minInterval = Math.max(0L, configSnapshot.topUpsertMinIntervalMs());
        if (prev != null && prev.hash.equals(hash) && (now - prev.lastWriteAt) < minInterval) {
            dbLog("DB skip topList (unchanged within interval): " + statKey);
            return;
        }
        int entries = (top == null) ? 0 : top.size();
        dbLog("Queue topList upsert: key=" + statKey + " entries=" + Math.min(entries, max) + " limit=" + max);
        topCache.put(statKey, new TopCacheEntry(hash, now));
        executor.submit(() -> {
            try {
                provider.upsertTopList(statKey, top, max);
                dbLog("Upserted topList: key=" + statKey + " entries=" + Math.min(entries, max));
            } catch (Exception e) {
                MyLogger.logWarning("Async upsertTopList failed: " + e.getMessage());
            }
        });
    }

    @Override
    public void close() {
        try {
            if (provider != null) provider.close();
        } catch (Exception ignored) { }
        shutdownExecutor();
    }

    private static final class NoopProvider implements DbProvider {
        @Override public void init(DatabaseConfig config) { }
        @Override public void start() { }
        @Override public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) { }
        @Override public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize) { }
        @Override public void close() { }
    }

    // --- internals ---
    private void initExecutor(int threads) {
        initExecutor(threads, DEFAULT_QUEUE_CAPACITY);
    }

    private void initExecutor(int threads, int queueCapacity) {
        shutdownExecutor();
        final AtomicInteger idx = new AtomicInteger(1);
        ThreadFactory tf = r -> {
            Thread t = new Thread(r);
            t.setName("PlayerStats-DB-" + idx.getAndIncrement());
            t.setDaemon(true);
            return t;
        };
        int th = Math.max(1, threads);
        int qc = Math.max(100, queueCapacity);
        executor = new ThreadPoolExecutor(
                th,
                th,
                60L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(qc),
                tf,
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
        dbLog("Initialized DB async executor with " + th + " threads and queueCapacity=" + qc);
    }

    private void shutdownExecutor() {
        if (executor != null) {
            try {
                executor.shutdown();
                executor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException ignored) {
            } finally {
                try { executor.shutdownNow(); } catch (Exception ignored) {}
                executor = null;
            }
        }
    }

    private void dbLog(String msg) {
        if (configSnapshot != null && configSnapshot.verboseLogging()) {
            MyLogger.logLowLevelMsg(msg);
        }
    }

    private static String hashTop(LinkedHashMap<String, Integer> top, int limit) {
        if (top == null || top.isEmpty()) return "";
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (var e : top.entrySet()) {
            if (i++ >= limit) break;
            sb.append(e.getKey()).append(':').append(e.getValue() == null ? 0 : e.getValue()).append('|');
        }
        return Integer.toHexString(sb.toString().hashCode());
    }

    private static final class PlayerStatCacheEntry {
        final int value;
        final long lastWriteAt;
        PlayerStatCacheEntry(int value, long lastWriteAt) { this.value = value; this.lastWriteAt = lastWriteAt; }
    }

    private static final class TopCacheEntry {
        final String hash;
        final long lastWriteAt;
        TopCacheEntry(String hash, long lastWriteAt) { this.hash = hash; this.lastWriteAt = lastWriteAt; }
    }
}
