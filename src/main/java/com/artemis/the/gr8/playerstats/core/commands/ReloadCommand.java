package com.artemis.the.gr8.playerstats.core.commands;

import com.artemis.the.gr8.playerstats.core.multithreading.ThreadManager;

import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.jetbrains.annotations.NotNull;

public final class ReloadCommand implements CommandExecutor {

    private static ThreadManager threadManager;

    public ReloadCommand(ThreadManager threadManager) {
        ReloadCommand.threadManager = threadManager;
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, String[] args) {
        if (!sender.hasPermission("playerstats.reload")) {
            sender.sendMessage("§cYou don't have permission to use this command.");
            return true;
        }
        
        threadManager.startReloadThread(sender);
        return true;
    }
}