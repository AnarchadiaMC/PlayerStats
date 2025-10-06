package com.artemis.the.gr8.playerstats.core;
import com.artemis.the.gr8.playerstats.api.PlayerStats;
import com.artemis.the.gr8.playerstats.api.RequestGenerator;
import com.artemis.the.gr8.playerstats.api.StatNumberFormatter;
import com.artemis.the.gr8.playerstats.api.StatTextFormatter;
import com.artemis.the.gr8.playerstats.api.StatManager;
import com.artemis.the.gr8.playerstats.core.commands.StatCommand;
import com.artemis.the.gr8.playerstats.core.commands.ExcludeCommand;
import com.artemis.the.gr8.playerstats.core.commands.ReloadCommand;
import com.artemis.the.gr8.playerstats.core.commands.ShareCommand;
import com.artemis.the.gr8.playerstats.core.commands.TabCompleter;
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
import com.artemis.the.gr8.playerstats.core.utils.PlayerDataReader;
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
import org.bukkit.OfflinePlayer;
import org.bukkit.World;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;

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
                long startTime = System.currentTimeMillis();
                List<String> keys = new ArrayList<>(dbm.trackedStatKeys());
                
                // Process stats in parallel using ForkJoinPool
                List<String> validKeys = keys.stream()
                    .filter(StatKeyUtil::isValidTrackedFormat)
                    .toList();
                
                MyLogger.logMediumLevelMsg("Generating top lists for " + validKeys.size() + " stats using parallel processing...");
                
                // Use parallel stream to process multiple stats concurrently
                validKeys.parallelStream().forEach(key -> {
                    try {
                        processTopListForKey(key, dbm);
                    } catch (Exception e) {
                        if (!isShuttingDown()) {
                            MyLogger.logWarning("Failed to generate top list for key '" + key + "': " + e.getMessage());
                        }
                    }
                });
                
                MyLogger.logMediumLevelTask("Top list generation completed", startTime);
            } catch (Exception e) {
                MyLogger.logWarning("Failed to generate top lists: " + e.getMessage());
            }
        });
    }

    /**
     * Process a single stat key to generate and store its top list.
     * This method is designed to be called concurrently from multiple threads.
     */
    private void processTopListForKey(String key, DatabaseManager dbm) {
        String[] parts = key.split(":");
        if (parts.length < 2) return;
        
        String type = parts[0];
        String statName = parts[1];
        
        try {
            Statistic stat = Statistic.valueOf(statName);
            RequestGenerator<LinkedHashMap<String, Integer>> gen = statManager.createTopStatRequest(dbm.config().topListSize());
            com.artemis.the.gr8.playerstats.api.StatRequest<LinkedHashMap<String, Integer>> req;
            
            switch (type) {
                case "UNTYPED" -> req = gen.untyped(stat);
                case "BLOCK", "ITEM" -> {
                    if (parts.length < 3) return;
                    try {
                        Material material = Material.valueOf(parts[2]);
                        req = gen.blockOrItemType(stat, material);
                    } catch (IllegalArgumentException e) {
                        MyLogger.logWarning("Invalid material in stat key '" + key + "': " + parts[2]);
                        return;
                    }
                }
                case "ENTITY" -> {
                    if (parts.length < 3) return;
                    try {
                        EntityType entityType = EntityType.valueOf(parts[2]);
                        req = gen.entityType(stat, entityType);
                    } catch (IllegalArgumentException e) {
                        MyLogger.logWarning("Invalid entity type in stat key '" + key + "': " + parts[2]);
                        return;
                    }
                }
                default -> { return; }
            }
            
            LinkedHashMap<String, Integer> top = statManager.executeTopRequest(req).value();
            dbm.upsertTopList(key, top);
        } catch (IllegalArgumentException e) {
            MyLogger.logWarning("Invalid enum value in stat key '" + key + "': " + e.getMessage());
        }
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
                        // First update offline experience data, then generate top lists
                        populateExperienceDataAsync(dbm);
                        generateTopListsAsync(dbm);
                    }
                } catch (Exception e) {
                    MyLogger.logWarning("Failed to generate periodic top lists: " + e.getMessage());
                }
            }
        }.runTaskTimerAsynchronously(this, periodTicks, periodTicks);
    }

    /**
     * Read all player data files and populate the experience database.
     * This is done asynchronously using multi-threading.
     */
    private void populateExperienceDataAsync(DatabaseManager dbm) {
        Bukkit.getScheduler().runTaskAsynchronously(this, () -> {
            try {
                long startTime = System.currentTimeMillis();
                MyLogger.logMediumLevelMsg("Starting experience data population from player files...");
                
                // Get the world directory (use the first world)
                World world = Bukkit.getWorlds().get(0);
                File worldDir = world.getWorldFolder();
                
                // Use a ConcurrentHashMap to store results from multi-threaded reading
                ConcurrentHashMap<java.util.UUID, PlayerDataReader.ExperienceData> expDataMap = new ConcurrentHashMap<>();
                
                // Use ForkJoinPool for parallel reading of player files
                int playerCount = ForkJoinPool.commonPool().submit(() -> 
                    PlayerDataReader.readAllPlayerExperience(worldDir, expDataMap)
                ).join();
                
                MyLogger.logMediumLevelMsg("Read experience data from " + playerCount + " player files");
                
                // Now write the data to the database asynchronously
                int written = 0;
                for (var entry : expDataMap.entrySet()) {
                    java.util.UUID uuid = entry.getKey();
                    PlayerDataReader.ExperienceData expData = entry.getValue();
                    
                    // Get player name from OfflinePlayer
                    OfflinePlayer offlinePlayer = Bukkit.getOfflinePlayer(uuid);
                    String playerName = offlinePlayer.getName();
                    if (playerName == null || playerName.isEmpty()) {
                        playerName = uuid.toString().substring(0, 8); // Use first 8 chars of UUID as fallback
                    }
                    
                    dbm.updatePlayerExperience(uuid, playerName, expData.level(), expData.totalExperience(), expData.expProgress());
                    written++;
                }
                
                long elapsed = System.currentTimeMillis() - startTime;
                MyLogger.logMediumLevelMsg("Experience population complete: wrote " + written + " entries in " + elapsed + "ms");
            } catch (Exception e) {
                MyLogger.logWarning("Failed to populate experience data: " + e.getMessage());
            }
        });
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

        // Optionally populate experience data from player files on startup
        if (dbm.config().enabled()) {
            populateExperienceDataAsync(dbm);
        }
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