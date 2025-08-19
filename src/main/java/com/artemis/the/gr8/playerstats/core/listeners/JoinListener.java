package com.artemis.the.gr8.playerstats.core.listeners;

import com.artemis.the.gr8.playerstats.core.Main;
import com.artemis.the.gr8.playerstats.core.db.DatabaseManager;
import com.artemis.the.gr8.playerstats.core.db.StatKeyUtil;
import com.artemis.the.gr8.playerstats.core.multithreading.ThreadManager;
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
 * Listens for new Players that join, and reloads PlayerStats
 * if someone joins that hasn't joined before.
 */
@ApiStatus.Internal
public class JoinListener implements Listener {

    private static ThreadManager threadManager;

    public JoinListener(ThreadManager t) {
        threadManager = t;
    }

    @EventHandler
    public void onPlayerJoin(PlayerJoinEvent joinEvent) {
        Player player = joinEvent.getPlayer();
        if (!player.hasPlayedBefore()) {
            threadManager.startReloadThread(null);
        }
        // Async DB update of tracked stats on join
        updateTrackedStatsAsync(player);
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
                    // skip invalid enum names
                } catch (Exception ignored) {
                }
            }
        });
    }
}