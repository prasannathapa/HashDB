package utils;

import java.util.*;

public class RandomUtil {
    public static final String[] cat = {"Exploit", "Malware", "Phishing", "Spam", "DDoS", "Virus", "Worm", "Ransomware", "Trojan", "Rootkit", "Keylogger", "Botnet"};
    public static final LinkedHashSet<String> allCategories = new LinkedHashSet<>(List.of(cat));
    public static String generateRandomIP() {
        Random random = new Random();
        StringBuilder ipBuilder = new StringBuilder();

        for (int i = 0; i < 4; i++) {
            ipBuilder.append(random.nextInt(256));
            if (i < 3) {
                ipBuilder.append(".");
            }
        }

        return ipBuilder.toString();
    }
    public static Set<String> getRandomCategories(int seed) {
        int[] indices = new int[3];
        int baseIndex = Math.abs(seed) % cat.length;
        for (int i = 0; i < 3; i++) {
            indices[i] = (baseIndex + i * 7) % cat.length; // Offset by a prime number (7)
            while (i > 0 && indices[i] == indices[i - 1]) {
                indices[i] = (indices[i] + 1) % cat.length;
            }
        }
        Set<String> selectedCategories = new HashSet<>(3,0.3f);
        for (int i = 0; i < 3; i++) {
            selectedCategories.add(cat[indices[i]]);
        }
        return selectedCategories;
    }
    public static long nsToMilliseconds(long ns) {
        return ns / 1_000_000;
    }

    public static String formatNumber(double number) {
        if (number >= 1_000_000_000) {
            return String.format("%.2fB", number / 1_000_000_000.0);
        } else if (number >= 1_000_000) {
            return String.format("%.2fM", number / 1_000_000.0);
        } else if (number >= 1_00_000) {
            return String.format("%.2fL", number / 1_00_000);
        } else if (number >= 1_000) {
            return String.format("%.2fK", number / 1_000.0);
        } else {
            return String.valueOf(number);
        }
    }

    public static double nsToSeconds(long ns) {
        return ns / 1_000_000_000.0;
    }

    public static void printStats(int itr, long op, long ns) {
        if (itr == 1) {
            System.out.printf("%-10s %-15s %-20s %-15s%n", "Iteration", "Duration", "Operation", "Throughput");
        }
        System.out.printf("%-10d %-15s %-20s %-15s%n", itr, nsToMilliseconds(ns) + " ms", formatNumber((double) ns / op) + " ns/op", formatNumber((double) op / nsToSeconds(ns)) + " op/sec");
    }

}
