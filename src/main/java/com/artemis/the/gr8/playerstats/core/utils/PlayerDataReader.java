package com.artemis.the.gr8.playerstats.core.utils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reads player data files (NBT format) to extract experience information for offline players.
 * This is used for initial population of the experience database on plugin startup.
 */
public final class PlayerDataReader {

    private PlayerDataReader() {}

    /**
     * Represents experience data for a player.
     */
    public record ExperienceData(int level, int totalExperience, float expProgress) {
        public ExperienceData {
            if (level < 0) level = 0;
            if (totalExperience < 0) totalExperience = 0;
            if (expProgress < 0.0f) expProgress = 0.0f;
            if (expProgress > 1.0f) expProgress = 1.0f;
        }
    }

    /**
     * Read experience data from all player data files in the world's playerdata directory.
     * This operation is designed to be run asynchronously.
     *
     * @param worldDirectory The world directory (typically obtained from Bukkit.getWorlds().get(0).getWorldFolder())
     * @param resultMap The concurrent map to store results in (UUID -> ExperienceData)
     * @return Number of player files successfully read
     */
    public static int readAllPlayerExperience(File worldDirectory, ConcurrentHashMap<UUID, ExperienceData> resultMap) {
        File playerDataDir = new File(worldDirectory, "playerdata");
        if (!playerDataDir.exists() || !playerDataDir.isDirectory()) {
            MyLogger.logWarning("Player data directory not found: " + playerDataDir.getAbsolutePath());
            return 0;
        }

        File[] playerFiles = playerDataDir.listFiles((dir, name) -> name.endsWith(".dat"));
        if (playerFiles == null || playerFiles.length == 0) {
            MyLogger.logLowLevelMsg("No player data files found in " + playerDataDir.getAbsolutePath());
            return 0;
        }

        int successCount = 0;
        int failedCount = 0;
        for (File playerFile : playerFiles) {
            try {
                String fileName = playerFile.getName();
                String uuidString = fileName.substring(0, fileName.length() - 4); // Remove .dat
                UUID uuid = UUID.fromString(uuidString);

                ExperienceData expData = readPlayerExperience(playerFile);
                if (expData != null) {
                    resultMap.put(uuid, expData);
                    successCount++;
                } else {
                    failedCount++;
                }
            } catch (IllegalArgumentException e) {
                // Invalid UUID in filename - skip silently (likely corrupted or very old file)
                failedCount++;
            } catch (StringIndexOutOfBoundsException e) {
                // Malformed filename - skip silently
                failedCount++;
            } catch (Exception e) {
                // Any other error - skip silently to continue processing
                failedCount++;
            }
        }

        if (failedCount > 0) {
            MyLogger.logLowLevelMsg("Skipped " + failedCount + " player files due to invalid format or errors");
        }
        return successCount;
    }

    /**
     * Read experience data from a single player data file using direct NBT reading.
     *
     * @param playerFile The player data file (.dat)
     * @return ExperienceData or null if reading failed
     */
    private static ExperienceData readPlayerExperience(File playerFile) {
        // Directly read NBT data without validation - errors handled by caller
        return readNBTData(playerFile);
    }

    /**
     * Manually read NBT data from player file.
     * This uses a simple approach to extract specific NBT tags without full NBT library.
     */
    private static ExperienceData readNBTData(File playerFile) {
        try (DataInputStream dis = new DataInputStream(new FileInputStream(playerFile))) {
            // Player data files are NBT compressed with GZip
            java.util.zip.GZIPInputStream gzipStream = new java.util.zip.GZIPInputStream(dis);
            DataInputStream nbtStream = new DataInputStream(gzipStream);
            
            // Read the NBT structure - this is simplified and looks for specific tags
            // Full NBT parsing would require a complete NBT library
            byte[] data = nbtStream.readAllBytes();
            
            // Search for experience-related NBT tags in the byte stream
            // NBT format: type byte, name length (short), name bytes, value
            int level = findIntTag(data, "XpLevel");
            int totalExp = findIntTag(data, "XpTotal");
            float expProgress = findFloatTag(data, "XpP");
            
            if (level >= 0) {
                return new ExperienceData(level, totalExp, expProgress);
            }
        } catch (Exception e) {
            // Silently fail for individual files - bulk operation
        }
        return null;
    }

    /**
     * Find an integer NBT tag in raw NBT data.
     * This is a simplified search that looks for the tag name followed by the int value.
     */
    private static int findIntTag(byte[] data, String tagName) {
        try {
            byte[] searchBytes = tagName.getBytes("UTF-8");
            for (int i = 0; i < data.length - searchBytes.length - 5; i++) {
                // Check if we found the tag name
                boolean match = true;
                for (int j = 0; j < searchBytes.length; j++) {
                    if (data[i + j] != searchBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    // Found the tag, read the next int (4 bytes, big endian)
                    int pos = i + searchBytes.length;
                    if (pos + 4 <= data.length) {
                        return ((data[pos] & 0xFF) << 24) |
                               ((data[pos + 1] & 0xFF) << 16) |
                               ((data[pos + 2] & 0xFF) << 8) |
                               (data[pos + 3] & 0xFF);
                    }
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        return -1;
    }

    /**
     * Find a float NBT tag in raw NBT data.
     */
    private static float findFloatTag(byte[] data, String tagName) {
        try {
            byte[] searchBytes = tagName.getBytes("UTF-8");
            for (int i = 0; i < data.length - searchBytes.length - 5; i++) {
                boolean match = true;
                for (int j = 0; j < searchBytes.length; j++) {
                    if (data[i + j] != searchBytes[j]) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    int pos = i + searchBytes.length;
                    if (pos + 4 <= data.length) {
                        int bits = ((data[pos] & 0xFF) << 24) |
                                   ((data[pos + 1] & 0xFF) << 16) |
                                   ((data[pos + 2] & 0xFF) << 8) |
                                   (data[pos + 3] & 0xFF);
                        return Float.intBitsToFloat(bits);
                    }
                }
            }
        } catch (Exception e) {
            // Ignore
        }
        return 0.0f;
    }
}
