package com.artemis.the.gr8.playerstats.core;

import com.artemis.the.gr8.playerstats.api.PlayerStats;
import com.artemis.the.gr8.playerstats.api.RequestGenerator;
import com.artemis.the.gr8.playerstats.api.StatNumberFormatter;
import com.artemis.the.gr8.playerstats.api.StatTextFormatter;
import com.artemis.the.gr8.playerstats.api.StatManager;
import com.artemis.the.gr8.playerstats.core.commands.*;
import com.artemis.the.gr8.playerstats.core.msg.msgutils.NumberFormatter;
import com.artemis.the.gr8.playerstats.core.config.ConfigHandler;
import com.artemis.the.gr8.playerstats.core.db.DatabaseManager;
import com.artemis.the.gr8.playerstats.core.listeners.JoinListener;
import com.artemis.the.gr8.playerstats.core.multithreading.ThreadManager;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import com.artemis.the.gr8.playerstats.core.msg.msgutils.LanguageKeyHandler;
import com.artemis.the.gr8.playerstats.core.sharing.ShareManager;
import com.artemis.the.gr8.playerstats.core.statistic.StatRequestManager;
import com.artemis.the.gr8.playerstats.core.utils.Closable;
import com.artemis.the.gr8.playerstats.core.utils.OfflinePlayerHandler;
import com.artemis.the.gr8.playerstats.core.utils.Reloadable;
import com.artemis.the.gr8.playerstats.core.db.StatKeyUtil;
import com.artemis.the.gr8.playerstats.core.msg.OutputManager;
import me.clip.placeholderapi.PlaceholderAPIPlugin;
import me.clip.placeholderapi.expansion.PlaceholderExpansion;

import org.bstats.bukkit.Metrics;
import org.bstats.charts.SimplePie;
import org.bukkit.Bukkit;
import org.bukkit.command.PluginCommand;
import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitRunnable;
import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.bukkit.Statistic;
import org.bukkit.Material;
import org.bukkit.entity.EntityType;

/**
 * PlayerStats' Main class
 */
public final class Main extends JavaPlugin implements PlayerStats {

    private static JavaPlugin pluginInstance;
    private static PlayerStats playerStatsAPI;
    private static ConfigHandler config;

    private static ThreadManager threadManager;
    private static StatRequestManager statManager;

    private static List<Reloadable> reloadables;
    private static List<Closable> closables;

    private static boolean shuttingDown = false;

    public static boolean isShuttingDown() {
        return shuttingDown;
    }

    @Override
    public void onEnable() {
        reloadables = new ArrayList<>();
        closables = new ArrayList<>();

        initializeMainClassesInOrder();
        registerCommands();
        setupMetrics();

        //register the listener
        Bukkit.getPluginManager().registerEvents(new JoinListener(threadManager), this);
        
        //finish up
        this.getLogger().info("Enabled PlayerStats!");
    }

    private void generateTopListsAsync(DatabaseManager dbm) {
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                for (String key : dbm.trackedStatKeys()) {
                    if (!StatKeyUtil.isValidTrackedFormat(key)) continue;
                    String[] parts = key.split(":");
                    String type = parts[0];
                    String statName = parts[1];
                    try {
                        Statistic stat = Statistic.valueOf(statName);
                        RequestGenerator<LinkedHashMap<String, Integer>> gen = statManager.createTopStatRequest(dbm.config().topListSize());
                        com.artemis.the.gr8.playerstats.api.StatRequest<LinkedHashMap<String, Integer>> req;
                        switch (type) {
                            case "UNTYPED" -> req = gen.untyped(stat);
                            case "BLOCK" -> {
                                try {
                                    Material material = Material.valueOf(parts[2]);
                                    req = gen.blockOrItemType(stat, material);
                                } catch (IllegalArgumentException e) {
                                    MyLogger.logWarning("Invalid material in stat key '" + key + "': " + parts[2]);
                                    continue;
                                }
                            }
                            case "ITEM" -> {
                                try {
                                    Material material = Material.valueOf(parts[2]);
                                    req = gen.blockOrItemType(stat, material);
                                } catch (IllegalArgumentException e) {
                                    MyLogger.logWarning("Invalid material in stat key '" + key + "': " + parts[2]);
                                    continue;
                                }
                            }
                            case "ENTITY" -> {
                                try {
                                    EntityType entityType = EntityType.valueOf(parts[2]);
                                    req = gen.entityType(stat, entityType);
                                } catch (IllegalArgumentException e) {
                                    MyLogger.logWarning("Invalid entity type in stat key '" + key + "': " + parts[2]);
                                    continue;
                                }
                            }
                            default -> { continue; }
                        }
                        LinkedHashMap<String, Integer> top = statManager.executeTopRequest(req).value();
                        dbm.upsertTopList(key, top);
                    } catch (IllegalArgumentException e) {
                        MyLogger.logWarning("Invalid enum value in stat key '" + key + "': " + e.getMessage());
                    } catch (Exception e) {
                        if (!isShuttingDown()) {
                            MyLogger.logWarning("Failed to generate top list for key '" + key + "': " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                MyLogger.logWarning("Failed to generate top lists: " + e.getMessage());
            }
        });
    }

    private void schedulePeriodicTopLists(DatabaseManager dbm) {
        if (!config.dbGenerateTopPeriodically()) return;
        int minutes = Math.max(1, config.dbGenerateTopIntervalMinutes());
        long periodTicks = minutes * 60L * 20L;
        new BukkitRunnable() {
            @Override
            public void run() {
                try {
                    if (dbm.config().enabled()) {
                        generateTopListsAsync(dbm);
                    }
                } catch (Exception e) {
                    MyLogger.logWarning("Failed to generate periodic top lists: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, periodTicks, periodTicks);
    }

    @Override
    public void onDisable() {
        shuttingDown = true;
        closables.forEach(Closable::close);
        this.getLogger().info("Disabled PlayerStats!");
    }

    public void reloadPlugin() {
        //config is not registered as reloadable to ensure it can be reloaded before everything else
        config.reload();
        reloadables.forEach(Reloadable::reload);
    }

    public static void registerReloadable(Reloadable reloadable) {
        reloadables.add(reloadable);
    }

    public static void registerClosable(Closable closable) {
        closables.add(closable);
    }

    /**
     *
     * @return the JavaPlugin instance associated with PlayerStats
     * @throws IllegalStateException if PlayerStats is not enabled
     */
    public static @NotNull JavaPlugin getPluginInstance() throws IllegalStateException {
        if (pluginInstance == null) {
            throw new IllegalStateException("PlayerStats is not loaded!");
        }
        return pluginInstance;
    }

    public static @NotNull PlayerStats getPlayerStatsAPI() throws IllegalStateException {
        if (playerStatsAPI == null) {
            throw new IllegalStateException("PlayerStats does not seem to be loaded!");
        }
        return playerStatsAPI;
    }

    /**
     * Initialize all classes that need initializing,
     * and store references to classes that are
     * needed for the Command classes or the API.
     */
    private void initializeMainClassesInOrder() {
        pluginInstance = this;
        playerStatsAPI = this;
        config = ConfigHandler.getInstance();

        LanguageKeyHandler.getInstance();
        OfflinePlayerHandler.getInstance();
        OutputManager.getInstance();
        ShareManager.getInstance();

        statManager = new StatRequestManager();
        threadManager = new ThreadManager(this);

        // Initialize and register DatabaseManager for reload and close hooks
        DatabaseManager dbm = DatabaseManager.getInstance();
        registerClosable(dbm);
        registerReloadable(dbm::reloadFromConfig);

        // Optionally generate top lists on load
        if (dbm.config().enabled() && dbm.config().generateTopOnLoad()) {
            generateTopListsAsync(dbm);
        }
        // Optionally schedule periodic generation of top lists
        schedulePeriodicTopLists(dbm);
    }

    /**
     * Register all commands and assign the tabCompleter
     * to the relevant commands.
     */
    private void registerCommands() {
        TabCompleter tabCompleter = new TabCompleter();

        PluginCommand statcmd = this.getCommand("statistic");
        if (statcmd != null) {
            statcmd.setExecutor(new StatCommand(threadManager));
            statcmd.setTabCompleter(tabCompleter);
        }
        PluginCommand excludecmd = this.getCommand("statisticexclude");
        if (excludecmd != null) {
            excludecmd.setExecutor(new ExcludeCommand());
            excludecmd.setTabCompleter(tabCompleter);
        }

        PluginCommand reloadcmd = this.getCommand("statisticreload");
        if (reloadcmd != null) {
            reloadcmd.setExecutor(new ReloadCommand(threadManager));
        }
        PluginCommand sharecmd = this.getCommand("statisticshare");
        if (sharecmd != null) {
            sharecmd.setExecutor(new ShareCommand());
        }
    }

    /**
     * Setup bstats
     */
    private void setupMetrics() {
        new BukkitRunnable() {
            @Override
            public void run() {
                final Metrics metrics = new Metrics(pluginInstance, 15923);
                final boolean placeholderExpansionActive;
                if (Bukkit.getPluginManager().isPluginEnabled("PlaceholderAPI")) {
                    PlaceholderExpansion expansion = PlaceholderAPIPlugin
                            .getInstance()
                            .getLocalExpansionManager()
                            .getExpansion("playerstats");
                    placeholderExpansionActive = expansion != null;
                } else {
                    placeholderExpansionActive = false;
                }
                metrics.addCustomChart(new SimplePie("using_placeholder_expansion", () -> placeholderExpansionActive ? "yes" : "no"));
            }
        }.runTaskLaterAsynchronously(this, 200);
    }

    @Override
    public @NotNull String getVersion() {
        return String.valueOf(this.getDescription().getVersion().charAt(0));
    }

    @Override
    public StatManager getStatManager() {
        return statManager;
    }

    @Override
    public StatTextFormatter getStatTextFormatter() {
        return OutputManager.getInstance().getMainMessageBuilder();
    }

    @Contract(" -> new")
    @Override
    public @NotNull StatNumberFormatter getStatNumberFormatter() {
        return new NumberFormatter();
    }
}