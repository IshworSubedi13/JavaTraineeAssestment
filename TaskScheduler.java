package com.ishworsubedi;
import java.time.Instant;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Scanner;

public class TaskScheduler {
    private static class Task {
        String id;
        long epochSeconds;
        String description;

        Task(String id, long epochSeconds, String description) {
            this.id = id;
            this.epochSeconds = epochSeconds;
            this.description = description;
        }
    }
    private final PriorityQueue<Task> queue = new PriorityQueue<>(
            Comparator.comparingLong((Task t) -> t.epochSeconds)
    );

    public synchronized void scheduleTask(String id, long epochSeconds, String description) {
        queue.offer(new Task(id, epochSeconds, description));
    }


    public synchronized void executeDueTasks() {
        long now = Instant.now().getEpochSecond();
        boolean executedAny = false;

        while (!queue.isEmpty() && queue.peek().epochSeconds <= now) {
            Task task = queue.poll();
            System.out.printf("Executing task %s: %s\n", task.id, task.description);
            executedAny = true;
        }

        if (!executedAny) {
            System.out.println("No tasks due at " + now);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        TaskScheduler scheduler = new TaskScheduler();
        Scanner scanner = new Scanner(System.in);

        System.out.println("TaskScheduler");
        System.out.println("Type Commands as shown below:");
        System.out.println("  schedule <id> <epochSeconds> <description>");
        System.out.println("  execute");
        System.out.println("  exit");

        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine();
            if (line == null || line.isBlank()) continue;

            String[] parts = line.split(" ", 4);
            String cmd = parts[0].toLowerCase();

            try {
                switch (cmd) {
                    case "schedule" -> {
                        if (parts.length < 4) {
                            System.out.println("schedule <id> <epochSeconds> <description>");
                            continue;
                        }
                        String id = parts[1];
                        long epochSeconds = Long.parseLong(parts[2]);
                        String description = parts[3];
                        scheduler.scheduleTask(id, epochSeconds, description);
                        System.out.println("Scheduled task " + id + " at " + epochSeconds);
                    }
                    case "execute" -> scheduler.executeDueTasks();
                    case "exit" -> {
                        System.out.println("Exiting.");
                        return;
                    }
                    default -> System.out.println("Command not found: " + cmd);
                }
            } catch (Exception e) {
                System.out.println("Error: " + e.getMessage());
            }
            long now = java.time.Instant.now().getEpochSecond();
            scheduler.scheduleTask("t1", now + 2, "Backup database");
            scheduler.scheduleTask("t2", now + 2, "Send report email");
            scheduler.scheduleTask("late", now - 5, "Run missed task");

            for (int i = 0; i < 6; i++) {
                scheduler.executeDueTasks();
                Thread.sleep(1000);
            }
        }

    }
}

