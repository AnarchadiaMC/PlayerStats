package com.artemis.the.gr8.playerstats.core.commands;

import com.artemis.the.gr8.playerstats.core.sharing.ShareManager;
import com.artemis.the.gr8.playerstats.core.enums.StandardMessage;
import com.artemis.the.gr8.playerstats.core.msg.OutputManager;
import com.artemis.the.gr8.playerstats.core.sharing.StoredResult;
import com.artemis.the.gr8.playerstats.core.utils.MyLogger;
import org.bukkit.command.Command;
import org.bukkit.command.CommandExecutor;
import org.bukkit.command.CommandSender;
import org.jetbrains.annotations.NotNull;

public final class ShareCommand implements CommandExecutor {

    private static OutputManager outputManager;
    private static ShareManager shareManager;

    public ShareCommand() {
        outputManager = OutputManager.getInstance();
        shareManager = ShareManager.getInstance();
    }

    @Override
    public boolean onCommand(@NotNull CommandSender sender, @NotNull Command command, @NotNull String label, String[] args) {
        // Input validation: limit number of arguments
        if (args.length > 5) {
            sender.sendMessage("§cToo many arguments provided.");
            return true;
        }
        
        if (args.length == 0) {
            outputManager.sendFeedbackMsg(sender, StandardMessage.MISSING_STAT_NAME);
            return true;
        }
        
        // Input validation: check argument lengths
        for (String arg : args) {
            if (arg != null && arg.length() > 50) {
                sender.sendMessage("§cArgument too long: " + arg.substring(0, 20) + "...");
                return true;
            }
        }

        if (args.length == 1 && shareManager.isEnabled()) {
            int shareCode;
            try {
                shareCode = Integer.parseInt(args[0]);
            } catch (IllegalArgumentException e) {
                MyLogger.logException(e, "ShareCommand", "/statshare is being called without a valid share-code!");
                return false;
            }
            if (shareManager.requestAlreadyShared(shareCode)) {
                outputManager.sendFeedbackMsg(sender, StandardMessage.RESULTS_ALREADY_SHARED);
            }
            else if (shareManager.isOnCoolDown(sender.getName())) {
                outputManager.sendFeedbackMsg(sender, StandardMessage.STILL_ON_SHARE_COOLDOWN);
            }
            else if (shareManager.isRateLimited(sender.getName())) {
                sender.sendMessage("§cYou are sharing too frequently. Please wait before sharing again.");
            }
            else {
                StoredResult result = shareManager.getStatResult(sender.getName(), shareCode);
                if (result == null) {  //at this point the only possible cause of formattedComponent being null is the request being older than 25 player-requests ago
                    outputManager.sendFeedbackMsg(sender, StandardMessage.STAT_RESULTS_TOO_OLD);
                } else {
                    outputManager.sendToAllPlayers(result.formattedValue());
                }
            }
        }
        return true;
    }
}
