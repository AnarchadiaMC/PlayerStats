package com.artemis.the.gr8.playerstats.core.db;

import com.artemis.the.gr8.playerstats.api.StatRequest;
import org.bukkit.Material;
import org.bukkit.Statistic;
import org.bukkit.entity.EntityType;

import java.util.Locale;
import java.util.Objects;

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
        return switch (type) {
            case "UNTYPED" -> parts.length == 2;
            case "BLOCK", "ITEM", "ENTITY" -> parts.length == 3;
            default -> false;
        };
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
