package com.artemis.the.gr8.playerstats.core.db.postgres;

import com.artemis.the.gr8.playerstats.core.db.DatabaseConfig;
import com.artemis.the.gr8.playerstats.core.db.DbProvider;
import com.artemis.the.gr8.playerstats.core.db.StatKeyUtil;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.postgresql.util.PGobject;

import java.lang.Class;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * PostgreSQL implementation using JDBC + HikariCP.
 * Stores per-player stats as a JSONB map (statKey -> value), and top lists as JSONB arrays.
 */
public final class PostgresProvider implements DbProvider {

    private HikariDataSource dataSource;
    private String schema;
    private String playerTable;
    private String topTable;

    @Override
    public void init(DatabaseConfig config) {
        // Sanitize identifiers to avoid SQL injection through config
        this.schema = sanitizeIdent(config.pgSchema(), "public");
        this.playerTable = sanitizeIdent(config.pgPlayerTable(), "player_stats");
        this.topTable = sanitizeIdent(config.pgTopTable(), "top_stats");

        String jdbcUrl = buildJdbcUrl(config);
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(jdbcUrl);
        hc.setUsername(nullToEmpty(config.pgUser()));
        hc.setPassword(nullToEmpty(config.pgPassword()));
        int maxPool = Math.max(1, config.maxPoolSize());
        hc.setMaximumPoolSize(maxPool);
        hc.setMinimumIdle(Math.min(2, maxPool));
        hc.setConnectionTimeout(Math.max(1000L, config.connectionTimeoutMs()));
        hc.setPoolName("PlayerStats-PG-Pool");
        // Optional: schema via connection init SQL
        hc.setConnectionInitSql("SET search_path TO " + quotedIdent(schema));
        // Keep connections alive on some managed providers
        hc.setKeepaliveTime(300_000L); // 5 minutes
        // Helpful for diagnosing slow/leaking calls when verbose logging is enabled
        if (config.verboseLogging()) {
            hc.setLeakDetectionThreshold(20_000L);
            hc.setValidationTimeout(5_000L);
        }

        // Explicitly load the shaded PostgreSQL driver
        try {
            Class.forName("com.artemis.the.gr8.playerstats.lib.pg.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Shaded PostgreSQL driver not found. Ensure the JAR includes the relocated dependency.", e);
        }

        validateSecurity(config);

        this.dataSource = new HikariDataSource(hc);
        if (config.verboseLogging()) {
            MyLogger.logLowLevelMsg("PostgresProvider initialized for db='" + config.pgDatabase() + "' schema='" + schema + "'");
        }
    }

    @Override
    public void start() {
        // Create schema and tables if they do not exist
        try (Connection c = dataSource.getConnection(); Statement st = c.createStatement()) {
            st.execute("CREATE SCHEMA IF NOT EXISTS " + quotedIdent(schema));
            st.execute("CREATE TABLE IF NOT EXISTS " + qualified(playerTable) + " (" +
                    "uuid UUID PRIMARY KEY," +
                    "name TEXT NOT NULL," +
                    "updated_at BIGINT NOT NULL," +
                    "stats JSONB NOT NULL DEFAULT '{}'::jsonb," +
                    "exp_level INT DEFAULT 0," +
                    "exp_total INT DEFAULT 0," +
                    "exp_progress REAL DEFAULT 0.0" +
                    ")");
            st.execute("CREATE TABLE IF NOT EXISTS " + qualified(topTable) + " (" +
                    "stat_key TEXT PRIMARY KEY," +
                    "top_size INT NOT NULL," +
                    "updated_at BIGINT NOT NULL," +
                    "entries JSONB NOT NULL" +
                    ")");
            
            // Migrate existing tables - add experience columns if they don't exist
            try {
                st.execute("ALTER TABLE " + qualified(playerTable) + " ADD COLUMN IF NOT EXISTS exp_level INT DEFAULT 0");
                st.execute("ALTER TABLE " + qualified(playerTable) + " ADD COLUMN IF NOT EXISTS exp_total INT DEFAULT 0");
                st.execute("ALTER TABLE " + qualified(playerTable) + " ADD COLUMN IF NOT EXISTS exp_progress REAL DEFAULT 0.0");
            } catch (SQLException migrationEx) {
                // Log but don't fail - columns might already exist on older PostgreSQL versions
                MyLogger.logWarning("Postgres column migration: " + migrationEx.getMessage());
            }
            
            // Indexes for common lookups and maintenance
            st.execute("CREATE INDEX IF NOT EXISTS idx_" + tableOnly(playerTable) + "_updated_at ON " + qualified(playerTable) + " (updated_at)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_" + tableOnly(playerTable) + "_stats_gin ON " + qualified(playerTable) + " USING GIN (stats jsonb_path_ops)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_" + tableOnly(playerTable) + "_exp_level ON " + qualified(playerTable) + " (exp_level DESC)");
            st.execute("CREATE INDEX IF NOT EXISTS idx_" + tableOnly(topTable) + "_updated_at ON " + qualified(topTable) + " (updated_at)");
        } catch (SQLException e) {
            MyLogger.logWarning("Postgres schema/table initialization failed: " + e.getMessage());
        }
    }

    @Override
    public void updatePlayerStat(UUID uuid, String playerName, String statKey, int value) {
        if (dataSource == null) return;
        if (uuid == null) return;
        if (!StatKeyUtil.isValidTrackedFormat(statKey) || statKey.length() > 128) return;
        String safeName = sanitizePlayerName(playerName);
        long now = Instant.now().toEpochMilli();

        String sql = "INSERT INTO " + qualified(playerTable) +
                " (uuid, name, updated_at, stats) VALUES (?, ?, ?, jsonb_build_object(?, ?)) " +
                "ON CONFLICT (uuid) DO UPDATE SET " +
                "name = EXCLUDED.name, " +
                "updated_at = EXCLUDED.updated_at, " +
                "stats = COALESCE(" + tableOnly(playerTable) + ".stats, '{}'::jsonb) || EXCLUDED.stats";

        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, uuid);
            ps.setString(2, safeName);
            ps.setLong(3, now);
            ps.setString(4, statKey);
            ps.setInt(5, Math.max(0, value));
            ps.executeUpdate();
        } catch (SQLException e) {
            MyLogger.logWarning("Postgres updatePlayerStat failed: " + e.getMessage());
        }
    }

    @Override
    public void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize) {
        if (dataSource == null) return;
        if (!StatKeyUtil.isValidTrackedFormat(statKey) || statKey.length() > 128) return;
        int safeTopSize = Math.max(1, Math.min(1000, topSize));
        long now = Instant.now().toEpochMilli();

        String json = toTopEntriesJson(top, safeTopSize);
        String sql = "INSERT INTO " + qualified(topTable) +
                " (stat_key, top_size, updated_at, entries) VALUES (?, ?, ?, ?) " +
                "ON CONFLICT (stat_key) DO UPDATE SET " +
                "top_size = EXCLUDED.top_size, " +
                "updated_at = EXCLUDED.updated_at, " +
                "entries = EXCLUDED.entries";

        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, statKey);
            ps.setInt(2, safeTopSize);
            ps.setLong(3, now);
            PGobject jsonb = new PGobject();
            jsonb.setType("jsonb");
            jsonb.setValue(json);
            ps.setObject(4, jsonb);
            ps.executeUpdate();
        } catch (SQLException e) {
            MyLogger.logWarning("Postgres upsertTopList failed: " + e.getMessage());
        }
    }

    @Override
    public void updatePlayerExperience(UUID uuid, String playerName, int level, int totalExperience, float expProgress) {
        if (dataSource == null) return;
        if (uuid == null) return;
        String safeName = sanitizePlayerName(playerName);
        int safeLevel = Math.max(0, level);
        int safeTotalExp = Math.max(0, totalExperience);
        float safeProgress = Math.max(0.0f, Math.min(1.0f, expProgress));
        long now = Instant.now().toEpochMilli();

        String sql = "INSERT INTO " + qualified(playerTable) +
                " (uuid, name, updated_at, stats, exp_level, exp_total, exp_progress) VALUES (?, ?, ?, '{}'::jsonb, ?, ?, ?) " +
                "ON CONFLICT (uuid) DO UPDATE SET " +
                "name = EXCLUDED.name, " +
                "updated_at = EXCLUDED.updated_at, " +
                "exp_level = EXCLUDED.exp_level, " +
                "exp_total = EXCLUDED.exp_total, " +
                "exp_progress = EXCLUDED.exp_progress";

        try (Connection c = dataSource.getConnection(); PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setObject(1, uuid);
            ps.setString(2, safeName);
            ps.setLong(3, now);
            ps.setInt(4, safeLevel);
            ps.setInt(5, safeTotalExp);
            ps.setFloat(6, safeProgress);
            ps.executeUpdate();
        } catch (SQLException e) {
            MyLogger.logWarning("Postgres updatePlayerExperience failed: " + e.getMessage());
        }
    }

    @Override
    public void close() {
        if (dataSource != null) {
            try { dataSource.close(); } catch (Exception ignored) {}
            dataSource = null;
        }
    }

    // Helpers
    private static String sanitizeIdent(String s, String def) {
        if (s == null) return def;
        String cleaned = s.replaceAll("[^a-zA-Z0-9_]", "");
        return cleaned.isEmpty() ? def : cleaned;
    }

    private static String quotedIdent(String ident) {
        // Safe since we already sanitized to [a-zA-Z0-9_]
        return '"' + ident + '"';
    }

    private String qualified(String table) {
        return quotedIdent(schema) + "." + quotedIdent(table);
    }

    private static String tableOnly(String table) { return table; }

    private static String nullToEmpty(String s) { return s == null ? "" : s; }

    private static String sanitizePlayerName(String s) {
        if (s == null) return "";
        String cleaned = s.replaceAll("[\\p{Cntrl}]", "").trim();
        if (cleaned.length() > 32) cleaned = cleaned.substring(0, 32);
        return cleaned;
    }

    private static String buildJdbcUrl(DatabaseConfig cfg) {
        String host = (cfg.pgHost() == null || cfg.pgHost().isBlank()) ? "localhost" : cfg.pgHost().trim();
        int port = cfg.pgPort() > 0 ? cfg.pgPort() : 5432;
        String db = (cfg.pgDatabase() == null || cfg.pgDatabase().isBlank()) ? "postgres" : cfg.pgDatabase().trim();
        String sslMode = cfg.pgSsl() ? "require" : "disable";
        return String.format("jdbc:postgresql://%s:%d/%s?sslmode=%s", host, port, db, sslMode);
    }

    private static void validateSecurity(DatabaseConfig cfg) {
        // Basic validations and recommendations
        if (!cfg.pgSsl() && !"localhost".equalsIgnoreCase(String.valueOf(cfg.pgHost()))) {
            MyLogger.logWarning("PostgreSQL SSL is disabled and host is not localhost. Consider enabling SSL in production.");
        }
        if (cfg.pgPassword() != null && cfg.pgPassword().equalsIgnoreCase("postgres")) {
            MyLogger.logWarning("PostgreSQL password is set to a common default. Consider changing it.");
        }
    }
    
    private static String toTopEntriesJson(LinkedHashMap<String, Integer> top, int limit) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        if (top != null && !top.isEmpty()) {
            int i = 0;
            for (Map.Entry<String, Integer> e : top.entrySet()) {
                if (i >= limit) break;
                if (i > 0) sb.append(',');
                sb.append('{')
                        .append("\"name\":\"").append(jsonEscape(e.getKey())).append('\"')
                        .append(',')
                        .append("\"value\":").append(Math.max(0, e.getValue() == null ? 0 : e.getValue()))
                        .append('}');
                i++;
            }
        }
        sb.append(']');
        return sb.toString();
    }

    private static String jsonEscape(String s) {
        if (s == null) return "";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }
}
