import tools.LogGenerator;

import java.io.IOException;
import java.nio.file.*;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Stream;

public class Main {

    public static void main(String[] args) throws Exception {
        // Logdateien generieren
        LogGenerator generator = new LogGenerator(5, "logs", LocalDate.now().minusDays(4), 10, 50);
        generator.generateLogs();

        // Sequentielle Analyse
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

        // paralelle Analyse
        System.out.println("\nParallele Analyse (LogLevel-Zählung):");
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
        System.out.println("Gesamtergebnis (parallel): " + totalParallel);
        System.out.println("Zeit (parallel): " + (endParallel - startParallel) + " ms");

        // erweiterte Fehleranalyse
        System.out.println("\nErweiterte Fehleranalyse (WARN/ERROR + Fehlertypen):");
        List<Future<ErrorAnalysisResult>> errorFutures = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(Paths.get("."), "logs-*.log")) {
            for (Path path : stream) {
                errorFutures.add(executor.submit(new ErrorLogAnalyzerTask(path)));
            }
        }

        Map<String, Integer> totalLevels = new HashMap<>();
        Map<String, Integer> totalErrorTypes = new HashMap<>();

        for (Future<ErrorAnalysisResult> future : errorFutures) {
            ErrorAnalysisResult result = future.get();
            mergeCounts(totalLevels, result.levelCounts);
            mergeCounts(totalErrorTypes, result.errorTypes);

            System.out.println("LogLevel-Zählung: " + result.levelCounts);
            System.out.println("WARN/ERROR-Zeilen (max 5):");
            result.warnAndErrorLines.stream().limit(5).forEach(System.out::println);
            System.out.println("---");
        }

        executor.shutdown();
        System.out.println("Gesamtergebnis LogLevel: " + totalLevels);
        System.out.println("Fehlertypen-Zählung: " + totalErrorTypes);
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

    public static class ErrorAnalysisResult {
        public final Map<String, Integer> levelCounts;
        public final List<String> warnAndErrorLines;
        public final Map<String, Integer> errorTypes;

        public ErrorAnalysisResult(Map<String, Integer> levelCounts, List<String> warnAndErrorLines, Map<String, Integer> errorTypes) {
            this.levelCounts = levelCounts;
            this.warnAndErrorLines = warnAndErrorLines;
            this.errorTypes = errorTypes;
        }
    }

    public static class ErrorLogAnalyzerTask implements Callable<ErrorAnalysisResult> {
        private final Path file;

        public ErrorLogAnalyzerTask(Path file) {
            this.file = file;
        }

        @Override
        public ErrorAnalysisResult call() throws Exception {
            Map<String, Integer> levelCount = new HashMap<>();
            List<String> errorLines = new ArrayList<>();
            Map<String, Integer> errorTypes = new HashMap<>();
            List<String> knownErrors = List.of("NullPointerException", "FileNotFoundException", "SQLException", "OutOfMemoryError", "IllegalArgumentException", "ArrayIndexOutOfBoundsException", "SecurityException");

            try (Stream<String> lines = Files.lines(file)) {
                lines.forEach(line -> {
                    for (String level : List.of("TRACE", "DEBUG", "INFO", "WARN", "ERROR")) {
                        if (line.contains(" " + level + " ")) {
                            levelCount.merge(level, 1, Integer::sum);
                            if (level.equals("WARN") || level.equals("ERROR")) {
                                errorLines.add(line);
                                for (String keyword : knownErrors) {
                                    if (line.contains(keyword)) {
                                        errorTypes.merge(keyword, 1, Integer::sum);
                                    }
                                }
                            }
                            break;
                        }
                    }
                });
            }
            return new ErrorAnalysisResult(levelCount, errorLines, errorTypes);
        }
    }
}
