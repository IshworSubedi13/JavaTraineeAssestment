package com.ishworsubedi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ParallelCsvAggregator {
    private static final int DEFAULT_BATCH_SIZE = 2000;
    private static final int THREAD_POOL_SIZE = Runtime.getRuntime().availableProcessors();

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length < 1) {
            System.err.println("java User <sample_5k.csv> [2000]");
            System.exit(1);
        }

        Path inputFile = Path.of(args[0]);
        int batchSize = args.length >= 2 ? Integer.parseInt(args[1]) : DEFAULT_BATCH_SIZE;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try (BufferedReader reader = new BufferedReader(new FileReader(inputFile.toFile()))) {
            String headerLine = reader.readLine();
            if (headerLine == null) {
                System.err.println("Empty file");
                return;
            }
            String[] headers = headerLine.split(",");
            int userIdIndex = findColumnIndex(headers, "userId");
            int amountIndex = findColumnIndex(headers, "amount");

            if (userIdIndex == -1 || amountIndex == -1) {
                System.err.println("Missing required columns 'userId' or 'amount'");
                return;
            }

            List<String> batch = new ArrayList<>(batchSize);
            List<Future<Map<String, UserStats>>> futures = new ArrayList<>();

            String line;
            while ((line = reader.readLine()) != null) {
                batch.add(line);
                if (batch.size() >= batchSize) {
                    List<String> batchCopy = new ArrayList<>(batch);
                    futures.add(executor.submit(() -> processBatch(batchCopy, userIdIndex, amountIndex)));
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                List<String> batchCopy = new ArrayList<>(batch);
                futures.add(executor.submit(() -> processBatch(batchCopy, userIdIndex, amountIndex)));
            }

            Map<String, UserStats> finalResults = new ConcurrentHashMap<>();
            for (Future<Map<String, UserStats>> future : futures) {
                try {
                    Map<String, UserStats> batchResult = future.get();
                    batchResult.forEach((userId, stats) ->
                            finalResults.merge(userId, stats, UserStats::merge));
                } catch (ExecutionException e) {
                    System.err.println("Error: " + e.getCause());
                }
            }

            System.out.println("userId,sum,avg");
            finalResults.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(entry -> {
                        String userId = entry.getKey();
                        UserStats stats = entry.getValue();
                        BigDecimal avg = stats.sum.divide(BigDecimal.valueOf(stats.count), 6, RoundingMode.HALF_UP);
                        System.out.printf("%s,%.2f,%.2f%n", userId, stats.sum, avg);
                    });

        } finally {
            shutdownExecutor(executor);
        }
    }

    private static int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].trim().equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    private static Map<String, UserStats> processBatch(List<String> lines, int userIdIndex, int amountIndex) {
        Map<String, UserStats> localMap = new ConcurrentHashMap<>();

        for (String line : lines) {
            String[] cols = line.split(",");
            if (cols.length <= Math.max(userIdIndex, amountIndex)) {
                continue;
            }
            String userId = cols[userIdIndex].trim();
            String amountStr = cols[amountIndex].trim();

            if (userId.isEmpty() || amountStr.isEmpty()) {
                continue;
            }

            try {
                BigDecimal amount = new BigDecimal(amountStr);
                localMap.merge(userId, new UserStats(amount, 1), UserStats::merge);
            } catch (NumberFormatException e) {
                System.err.println("Invalid amount: " + amountStr);
            }
        }

        return localMap;
    }

    private static void shutdownExecutor(ExecutorService executor) throws InterruptedException {
        executor.shutdown();
        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
            executor.shutdownNow();
            if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Executor did not terminate");
            }
        }
    }

    private record UserStats(BigDecimal sum, int count) {

        UserStats merge(UserStats other) {
            return new UserStats(this.sum.add(other.sum), this.count + other.count);
        }
    }
}

