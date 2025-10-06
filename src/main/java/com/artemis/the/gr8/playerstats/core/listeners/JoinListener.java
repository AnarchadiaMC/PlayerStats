package com.artemis.the.gr8.playerstats.core.listeners;

import com.artemis.the.gr8.playerstats.core.Main;
import com.artemis.the.gr8.playerstats.core.db.DatabaseManager;
import com.artemis.the.gr8.playerstats.core.db.StatKeyUtil;
import com.artemis.the.gr8.playerstats.core.multithreading.ThreadManager;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import org.bukkit.Bukkit;
import org.bukkit.Material;
import org.bukkit.Statistic;
import org.bukkit.entity.EntityType;
import org.bukkit.entity.Player;
import org.bukkit.event.EventHandler;
import org.bukkit.event.Listener;
import org.bukkit.event.player.PlayerJoinEvent;
import org.jetbrains.annotations.ApiStatus;

/**
 * Listens for new Players that join and updates their statistics
 * in the database if enabled.
 */
@ApiStatus.Internal
public class JoinListener implements Listener {

    public JoinListener(ThreadManager t) {
        // ThreadManager no longer needed since we removed the plugin reload trigger
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent joinEvent) {
        Player player = joinEvent.getPlayer();
        // Only update database stats, no need to reload entire plugin for new players
        // The plugin should handle new players gracefully without full reload
        updateTrackedStatsAsync(player);
        updatePlayerExperienceAsync(player);
    }

    private void updateTrackedStatsAsync(Player player) {
        DatabaseManager dbm = DatabaseManager.getInstance();
        if (!dbm.config().enabled() || !dbm.config().updatePlayerOnJoin()) return;

        Bukkit.getScheduler().runTaskAsynchronously(Main.getPluginInstance(), () -> {
            for (String key : dbm.trackedStatKeys()) {
                if (!StatKeyUtil.isValidTrackedFormat(key)) continue;
                try {
                    String[] parts = key.split(":");
                    String type = parts[0];
                    String statName = parts[1];
                    Statistic stat = Statistic.valueOf(statName);
                    int value;
                    switch (type) {
                        case "UNTYPED" -> value = player.getStatistic(stat);
                        case "BLOCK" -> {
                            Material m = Material.valueOf(parts[2]);
                            value = player.getStatistic(stat, m);
                        }
                        case "ITEM" -> {
                            Material m = Material.valueOf(parts[2]);
                            value = player.getStatistic(stat, m);
                        }
                        case "ENTITY" -> {
                            EntityType e = EntityType.valueOf(parts[2]);
                            value = player.getStatistic(stat, e);
                        }
                        default -> {
                            continue;
                        }
                    }
                    dbm.updatePlayerStat(player.getUniqueId(), player.getName(), key, value);
                } catch (IllegalArgumentException ex) {
                    MyLogger.logWarning("Invalid enum in stat key '" + key + "': " + ex.getMessage());
                } catch (Exception e) {
                    MyLogger.logWarning("Failed to update stat '" + key + "' for player " + player.getName() + ": " + e.getMessage());
                }
            }
        });
    }

    private void updatePlayerExperienceAsync(Player player) {
        DatabaseManager dbm = DatabaseManager.getInstance();
        if (!dbm.config().enabled()) return;

        Bukkit.getScheduler().runTaskAsynchronously(Main.getPluginInstance(), () -> {
            try {
                int level = player.getLevel();
                int totalExp = player.getTotalExperience();
                float expProgress = player.getExp();
                dbm.updatePlayerExperience(player.getUniqueId(), player.getName(), level, totalExp, expProgress);
            } catch (Exception e) {
                MyLogger.logWarning("Failed to update experience for player " + player.getName() + ": " + e.getMessage());
            }
        });
    }
}