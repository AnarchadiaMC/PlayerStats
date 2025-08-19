package com.artemis.the.gr8.playerstats.core.db;

import java.util.LinkedHashMap;
import java.util.UUID;

public interface DbProvider extends AutoCloseable {

    void init(DatabaseConfig config) throws Exception;

    default void start() throws Exception { /* optional */ }

    void updatePlayerStat(UUID uuid, String playerName, String statKey, int value);

    void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize);

    @Override
    void close();
}
