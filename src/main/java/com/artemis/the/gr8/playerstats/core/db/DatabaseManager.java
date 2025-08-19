package com.artemis.the.gr8.playerstats.core.db;

import com.artemis.the.gr8.playerstats.core.config.ConfigHandler;
import com.artemis.the.gr8.playerstats.core.utils.Closable;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import com.artemis.the.gr8.playerstats.core.db.mongo.MongoDbProvider;
import com.artemis.the.gr8.playerstats.core.db.postgres.PostgresProvider;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

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
    private DbProvider provider;

    private DatabaseManager() {
        reloadFromConfig();
    }

    public void reloadFromConfig() {
        this.configSnapshot = DatabaseConfig.from(ConfigHandler.getInstance());
        // Validate tracked stat formats
        long invalid = this.configSnapshot.trackedStats().stream().filter(s -> !StatKeyUtil.isValidTrackedFormat(s)).count();
        if (invalid > 0) {
            MyLogger.logWarning("Database config contains " + invalid + " invalid tracked-stats entries. They will be ignored.");
        }
        if (!configSnapshot.enabled()) {
            provider = new NoopProvider();
            MyLogger.logLowLevelMsg("Database disabled in config; using No-Op provider.");
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
        } catch (Exception e) {
            MyLogger.logWarning("Failed to initialize DB provider; falling back to No-Op. " + e.getMessage());
            provider = new NoopProvider();
        }
    }

    public DatabaseConfig config() { return configSnapshot; }

    public List<String> trackedStatKeys() {
        return configSnapshot.trackedStats().stream()
                .filter(StatKeyUtil::isValidTrackedFormat)
                .collect(Collectors.toList());
    }

    public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) {
        provider.updatePlayerStat(uuid, playerName, statKey, value);
    }

    public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top) {
        int max = configSnapshot.topListSize();
        provider.upsertTopList(statKey, top, max);
    }

    @Override
    public void close() {
        try {
            if (provider != null) provider.close();
        } catch (Exception ignored) { }
    }

    private static final class NoopProvider implements DbProvider {
        @Override public void init(DatabaseConfig config) { }
        @Override public void start() { }
        @Override public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) { }
        @Override public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize) { }
        @Override public void close() { }
    }
}
