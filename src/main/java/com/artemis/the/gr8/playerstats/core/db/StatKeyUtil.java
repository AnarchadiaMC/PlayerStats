package com.artemis.the.gr8.playerstats.core.db;

import com.artemis.the.gr8.playerstats.api.StatRequest;
import org.bukkit.Material;
import org.bukkit.Statistic;
import org.bukkit.entity.EntityType;

import java.util.Locale;
import java.util.Objects;
import java.util.ArrayList;
import java.util.List;

public final class StatKeyUtil {

    private StatKeyUtil() {}

    public static String keyFor(StatRequest.Settings s) {
        Statistic stat = s.getStatistic();
        return switch (stat.getType()) {
            case UNTYPED -> join("UNTYPED", stat.name());
            case BLOCK -> join("BLOCK", stat.name(), nameOf(s.getBlock()));
            case ITEM -> join("ITEM", stat.name(), nameOf(s.getItem()));
            case ENTITY -> join("ENTITY", stat.name(), nameOf(s.getEntity()));
        };
    }

    public static boolean isValidTrackedFormat(String s) {
        if (s == null) return false;
        String[] parts = s.split(":");
        if (parts.length < 2 || parts.length > 3) return false;
        String type = parts[0].toUpperCase(Locale.ROOT);
        if (parts.length != (type.equals("UNTYPED") ? 2 : 3)) return false;
        if (!switch (type) {
            case "UNTYPED", "BLOCK", "ITEM", "ENTITY" -> true;
            default -> false;
        }) return false;

        String statName = parts[1].toUpperCase(Locale.ROOT);
        Statistic stat;
        try {
            stat = Statistic.valueOf(statName);
        } catch (IllegalArgumentException e) {
            return false;
        }

        return switch (type) {
            case "UNTYPED" -> stat.getType() == Statistic.Type.UNTYPED;
            case "BLOCK" -> stat.getType() == Statistic.Type.BLOCK;
            case "ITEM" -> stat.getType() == Statistic.Type.ITEM;
            case "ENTITY" -> stat.getType() == Statistic.Type.ENTITY;
            default -> false;
        };
    }

    /**
     * Enumerate all possible tracked keys across Bukkit enums.
     * This is intentionally exhaustive; downstream callers should handle
     * IllegalArgumentException for combos that aren't applicable.
     */
    public static List<String> enumerateAllKeys() {
        List<String> keys = new ArrayList<>();
        for (Statistic stat : Statistic.values()) {
            switch (stat.getType()) {
                case UNTYPED -> keys.add(join("UNTYPED", stat.name()));
                case BLOCK -> {
                    for (org.bukkit.Material m : org.bukkit.Material.values()) {
                        if (m.isBlock()) keys.add(join("BLOCK", stat.name(), m.name()));
                    }
                }
                case ITEM -> {
                    for (org.bukkit.Material m : org.bukkit.Material.values()) {
                        if (m.isItem()) keys.add(join("ITEM", stat.name(), m.name()));
                    }
                }
                case ENTITY -> {
                    for (EntityType e : EntityType.values()) {
                        if (e != EntityType.UNKNOWN && e.isAlive()) {
                            keys.add(join("ENTITY", stat.name(), e.name()));
                        }
                    }
                }
            }
        }
        return keys;
    }

    private static String nameOf(Material m) { return m == null ? "" : m.name(); }
    private static String nameOf(EntityType e) { return e == null ? "" : e.name(); }

    private static String join(String... parts) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < parts.length; i++) {
            if (i > 0) sb.append(':');
            sb.append(Objects.requireNonNullElse(parts[i], ""));
        }
        return sb.toString();
    }
}
