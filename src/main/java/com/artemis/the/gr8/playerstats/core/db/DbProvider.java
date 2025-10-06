package com.artemis.the.gr8.playerstats.core.db;

import java.util.LinkedHashMap;
import java.util.UUID;

public interface DbProvider extends AutoCloseable {

    void init(DatabaseConfig config) throws Exception;

    default void start() throws Exception { /* optional */ }

    void updatePlayerStat(UUID uuid, String playerName, String statKey, int value);

    void upsertTopList(String statKey, LinkedHashMap<String, Integer> top, int topSize);

    /**
     * Update player experience data in the database.
     *
     * @param uuid Player's UUID
     * @param playerName Player's name
     * @param level Experience level
     * @param totalExperience Total experience points
     * @param expProgress Progress toward next level (0.0 to 1.0)
     */
    void updatePlayerExperience(UUID uuid, String playerName, int level, int totalExperience, float expProgress);

    @Override
    void close();
}
