package com.artemis.the.gr8.playerstats.core.db;

import com.artemis.the.gr8.playerstats.core.config.ConfigHandler;

import java.util.List;
import java.util.Objects;

/** Immutable snapshot of database-related configuration. */
public final class DatabaseConfig {

    public enum DbType { MONGO, POSTGRES }

    private final boolean enabled;
    private final DbType type;

    private final boolean generateTopOnLoad;
    private final boolean generateTopPeriodically;
    private final int generateTopIntervalMinutes;
    private final boolean updatePlayerOnJoin;
    private final int topListSize;
    private final List<String> trackedStats;
    private final boolean verboseLogging;

    // Async and caching
    private final int asyncThreads;
    private final long playerUpdateMinIntervalMs;
    private final long topUpsertMinIntervalMs;

    // Pool
    private final int maxPoolSize;
    private final long connectionTimeoutMs;

    // Mongo
    private final String mongoUri;
    private final String mongoDatabase;
    private final String mongoPlayerCollection;
    private final String mongoTopCollection;

    // Postgres
    private final String pgHost;
    private final int pgPort;
    private final String pgDatabase;
    private final String pgUser;
    private final String pgPassword;
    private final String pgSchema;
    private final boolean pgSsl;
    private final String pgPlayerTable;
    private final String pgTopTable;

    private DatabaseConfig(
            boolean enabled, DbType type,
            boolean generateTopOnLoad, boolean generateTopPeriodically, int generateTopIntervalMinutes,
            boolean updatePlayerOnJoin, int topListSize,
            List<String> trackedStats,
            boolean verboseLogging,
            int asyncThreads, long playerUpdateMinIntervalMs, long topUpsertMinIntervalMs,
            int maxPoolSize, long connectionTimeoutMs,
            String mongoUri, String mongoDatabase, String mongoPlayerCollection, String mongoTopCollection,
            String pgHost, int pgPort, String pgDatabase, String pgUser, String pgPassword, String pgSchema, boolean pgSsl,
            String pgPlayerTable, String pgTopTable) {
        this.enabled = enabled;
        this.type = type;
        this.generateTopOnLoad = generateTopOnLoad;
        this.generateTopPeriodically = generateTopPeriodically;
        this.generateTopIntervalMinutes = generateTopIntervalMinutes;
        this.updatePlayerOnJoin = updatePlayerOnJoin;
        this.topListSize = topListSize;
        this.trackedStats = List.copyOf(trackedStats);
        this.verboseLogging = verboseLogging;
        this.asyncThreads = asyncThreads;
        this.playerUpdateMinIntervalMs = playerUpdateMinIntervalMs;
        this.topUpsertMinIntervalMs = topUpsertMinIntervalMs;
        this.maxPoolSize = maxPoolSize;
        this.connectionTimeoutMs = connectionTimeoutMs;
        this.mongoUri = mongoUri;
        this.mongoDatabase = mongoDatabase;
        this.mongoPlayerCollection = mongoPlayerCollection;
        this.mongoTopCollection = mongoTopCollection;
        this.pgHost = pgHost;
        this.pgPort = pgPort;
        this.pgDatabase = pgDatabase;
        this.pgUser = pgUser;
        this.pgPassword = pgPassword;
        this.pgSchema = pgSchema;
        this.pgSsl = pgSsl;
        this.pgPlayerTable = pgPlayerTable;
        this.pgTopTable = pgTopTable;
    }

    public static DatabaseConfig from(ConfigHandler c) {
        boolean enabled = c.dbEnabled();
        DbType type = parseType(c.dbType());
        return new DatabaseConfig(
                enabled,
                type,
                c.dbGenerateTopOnLoad(),
                c.dbGenerateTopPeriodically(),
                c.dbGenerateTopIntervalMinutes(),
                c.dbUpdatePlayerOnJoin(),
                c.dbTopListSize(),
                c.dbTrackedStats(),
                c.dbVerboseLogging(),
                c.dbAsyncThreads(),
                c.dbPlayerUpdateMinIntervalMs(),
                c.dbTopUpsertMinIntervalMs(),
                c.poolMaxSize(),
                c.poolConnectionTimeoutMs(),
                c.mongoUri(),
                c.mongoDatabase(),
                c.mongoPlayerCollection(),
                c.mongoTopCollection(),
                c.pgHost(),
                c.pgPort(),
                c.pgDatabase(),
                c.pgUser(),
                c.pgPassword(),
                c.pgSchema(),
                c.pgSsl(),
                c.pgPlayerTable(),
                c.pgTopTable()
        );
    }

    private static DbType parseType(String v) {
        if (v == null || v.isBlank()) return DbType.POSTGRES; // default to PostgreSQL
        return Objects.equals(v.trim().toLowerCase(), "postgres") ? DbType.POSTGRES : DbType.MONGO;
    }

    public boolean enabled() { return enabled; }
    public DbType type() { return type; }

    public boolean generateTopOnLoad() { return generateTopOnLoad; }
    public boolean generateTopPeriodically() { return generateTopPeriodically; }
    public int generateTopIntervalMinutes() { return generateTopIntervalMinutes; }
    public boolean updatePlayerOnJoin() { return updatePlayerOnJoin; }
    public int topListSize() { return topListSize; }
    public List<String> trackedStats() { return trackedStats; }
    public boolean verboseLogging() { return verboseLogging; }

    public int asyncThreads() { return asyncThreads; }
    public long playerUpdateMinIntervalMs() { return playerUpdateMinIntervalMs; }
    public long topUpsertMinIntervalMs() { return topUpsertMinIntervalMs; }

    public int maxPoolSize() { return maxPoolSize; }
    public long connectionTimeoutMs() { return connectionTimeoutMs; }

    public String mongoUri() { return mongoUri; }
    public String mongoDatabase() { return mongoDatabase; }
    public String mongoPlayerCollection() { return mongoPlayerCollection; }
    public String mongoTopCollection() { return mongoTopCollection; }

    public String pgHost() { return pgHost; }
    public int pgPort() { return pgPort; }
    public String pgDatabase() { return pgDatabase; }
    public String pgUser() { return pgUser; }
    public String pgPassword() { return pgPassword; }
    public String pgSchema() { return pgSchema; }
    public boolean pgSsl() { return pgSsl; }
    public String pgPlayerTable() { return pgPlayerTable; }
    public String pgTopTable() { return pgTopTable; }
}
