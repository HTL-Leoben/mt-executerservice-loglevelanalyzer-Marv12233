import tools.LogGenerator;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) throws Exception {
        // --- Schritt 1: Logdateien generieren ---
        LogGenerator generator = new LogGenerator(5, "logs", LocalDate.now().minusDays(4), 10, 50);
        generator.generateLogs();

        // --- Schritt 2: Sequentielle Analyse ---
        System.out.println("\nSequentielle Analyse:");
        long startSequential = System.currentTimeMillis();
        Map<String, Integer> totalSequential = new HashMap<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("."), "logs-*.log")) {
            for (Path path : stream) {
                Map<String, Integer> result = analyzeLogFile(path);
                System.out.println("Ergebnisse für Datei " + path.getFileName() + ": " + result);
                mergeCounts(totalSequential, result);
            }
        }
        long endSequential = System.currentTimeMillis();
        System.out.println("Gesamtergebnis (sequentiell): " + totalSequential);
        System.out.println("Zeit (sequentiell): " + (endSequential - startSequential) + " ms");

        // --- Schritt 3: Parallele Analyse ---
        System.out.println("\nParallele Analyse:");
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        List<Future<Map<String, Integer>>> futures = new ArrayList<>();

        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("."), "logs-*.log")) {
            for (Path path : stream) {
                futures.add(executor.submit(new LogAnalyzerTask(path)));
            }
        }

        Map<String, Integer> totalParallel = new HashMap<>();
        long startParallel = System.currentTimeMillis();
        for (Future<Map<String, Integer>> future : futures) {
            Map<String, Integer> result = future.get();
            System.out.println("Ergebnis einer Datei: " + result);
            mergeCounts(totalParallel, result);
        }
        long endParallel = System.currentTimeMillis();
        executor.shutdown();

        System.out.println("Gesamtergebnis (parallel): " + totalParallel);
        System.out.println("Zeit (parallel): " + (endParallel - startParallel) + " ms");
    }

    public static Map<String, Integer> analyzeLogFile(Path path) {
        Map<String, Integer> levelCount = new HashMap<>();
        try (Stream<String> lines = Files.lines(path)) {
            lines.forEach(line -> {
                for (String level : List.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR")) {
                    if (line.contains(" " + level + " ")) {
                        levelCount.merge(level, 1, Integer::sum);
                        break;
                    }
                }
            });
        } catch (IOException e) {
            System.err.println("Fehler beim Lesen von Datei " + path + ": " + e.getMessage());
        }
        return levelCount;
    }

    public static void mergeCounts(Map<String, Integer> total, Map<String, Integer> partial) {
        for (var entry : partial.entrySet()) {
            total.merge(entry.getKey(), entry.getValue(), Integer::sum);
        }
    }

    public static class LogAnalyzerTask implements Callable<Map<String, Integer>> {
        private final Path file;

        public LogAnalyzerTask(Path file) {
            this.file = file;
        }

        @Override
        public Map<String, Integer> call() {
            return analyzeLogFile(file);
        }
    }
}
