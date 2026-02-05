package coordinator.metrics;
import java.util.stream.Collectors;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Comprehensive Metrics Collector для MapReduce scheduling.
 *
 * Собирает ВСЕ метрики, требуемые для диплома:
 *
 * 1. Метрики загруженности узлов:
 *    - CPU load (proxy: время обработки)
 *    - Memory usage (до/после chunk)
 *    - Количество активных задач
 *    - Количество обработанных chunks
 *    - Среднее и скользящее среднее времени
 *
 * 2. Метрики состояния узлов:
 *    - idle / busy / overloaded
 *    - Частота простоя
 *    - Динамика состояния во времени
 *
 * 3. Метрики распределения:
 *    - Дисбаланс нагрузки (max-min)
 *    - Стандартное отклонение
 *    - Доля задач на weak/strong узлах
 *
 * 4. Метрики RL:
 *    - Rewards
 *    - Epsilon
 *    - Q-values
 *
 * 5. Метрики отказоустойчивости:
 *    - Время обнаружения отказа
 *    - Время восстановления
 *    - Перераспределённые chunks
 *
 * @author Expert Review
 */
public class MetricsCollector {

    private final Path runDir;
    private final String mode;
    private final long startTime;

    // Per-worker metrics
    private final Map<String, WorkerMetrics> workerMetrics = new ConcurrentHashMap<>();

    // System-level metrics over time
    private final List<SystemSnapshot> systemSnapshots = new ArrayList<>();

    // Chunk-level metrics
    private final List<ChunkMetric> chunkMetrics = new ArrayList<>();

    // RL-specific metrics
    private final List<RLMetric> rlMetrics = new ArrayList<>();

    // Fault tolerance metrics
    private final List<FaultEvent> faultEvents = new ArrayList<>();

    // Writers (lazy initialized)
    private PrintWriter chunkWriter;
    private PrintWriter systemWriter;
    private PrintWriter rlWriter;
    private PrintWriter workerStateWriter;
    private PrintWriter faultWriter;

    public MetricsCollector(Path runDir, String mode) {
        this.runDir = runDir;
        this.mode = mode;
        this.startTime = System.currentTimeMillis();

        try {
            Files.createDirectories(runDir.resolve("metrics"));
            initializeWriters();
        } catch (IOException e) {
            System.err.println("Failed to initialize metrics: " + e.getMessage());
        }
    }

    private void initializeWriters() throws IOException {
        // Chunk-level metrics
        chunkWriter = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/chunks.csv").toFile()));
        chunkWriter.println("timestamp_ms,chunk_id,worker,lines,processing_time_ms," +
                "worker_chunks_before,worker_avg_time_before,mem_before_mb,mem_after_mb," +
                "queue_size,system_imbalance,strategy");

        // System snapshots
        systemWriter = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/system.csv").toFile()));
        systemWriter.println("timestamp_ms,elapsed_ms,completed_chunks,dispatched_chunks," +
                "busy_workers,total_workers,imbalance,std_dev,throughput_lines_per_sec");

        // RL metrics
        rlWriter = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/rl.csv").toFile()));
        rlWriter.println("timestamp_ms,chunk_id,state,action,reward,epsilon," +
                "q_before,q_after,exploration");

        // Worker state over time
        workerStateWriter = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/worker_states.csv").toFile()));
        workerStateWriter.println("timestamp_ms,worker,state,chunks_completed," +
                "avg_time_ms,moving_avg_time_ms,idle_time_ms");

        // Fault events
        faultWriter = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/faults.csv").toFile()));
        faultWriter.println("timestamp_ms,event_type,worker,details," +
                "detection_time_ms,recovery_time_ms,chunks_redistributed");
    }

    // =================== WORKER REGISTRATION ===================

    public void registerWorker(String workerId) {
        workerMetrics.put(workerId, new WorkerMetrics(workerId));
        logWorkerState(workerId, "IDLE");
    }

    // =================== CHUNK METRICS ===================

    /**
     * Record chunk dispatch.
     */
    public void recordChunkDispatched(int chunkId, String worker, int lines,
                                      int queueSize, int systemImbalance) {
        WorkerMetrics wm = workerMetrics.get(worker);
        if (wm != null) {
            wm.markBusy();
        }

        long now = System.currentTimeMillis();
        chunkWriter.printf("%d,%d,%s,%d,0,%d,%.2f,0,0,%d,%d,%s%n",
                now, chunkId, worker, lines,
                wm != null ? wm.chunksCompleted : 0,
                wm != null ? wm.avgTime : 0,
                queueSize, systemImbalance, mode);
        chunkWriter.flush();
    }

    /**
     * Record chunk completion with full metrics.
     */
    public void recordChunkCompleted(int chunkId, String worker, int lines,
                                     long processingTimeMs, long memBeforeMB, long memAfterMB,
                                     int systemImbalance) {
        long now = System.currentTimeMillis();

        WorkerMetrics wm = workerMetrics.get(worker);
        if (wm != null) {
            wm.recordChunkCompletion(processingTimeMs, memBeforeMB, memAfterMB);
        }

        ChunkMetric cm = new ChunkMetric();
        cm.timestamp = now;
        cm.chunkId = chunkId;
        cm.worker = worker;
        cm.lines = lines;
        cm.processingTimeMs = processingTimeMs;
        cm.memBeforeMB = memBeforeMB;
        cm.memAfterMB = memAfterMB;
        cm.systemImbalance = systemImbalance;
        chunkMetrics.add(cm);

        chunkWriter.printf("%d,%d,%s,%d,%d,%d,%.2f,%d,%d,0,%d,%s%n",
                now, chunkId, worker, lines, processingTimeMs,
                wm != null ? wm.chunksCompleted : 0,
                wm != null ? wm.avgTime : 0,
                memBeforeMB, memAfterMB, systemImbalance, mode);
        chunkWriter.flush();

        logWorkerState(worker, wm != null && wm.isBusy ? "BUSY" : "IDLE");
    }

    // =================== SYSTEM METRICS ===================

    /**
     * Record system-level snapshot (call periodically).
     */
    public void recordSystemSnapshot(int completedChunks, int dispatchedChunks,
                                     int busyWorkers, int totalWorkers,
                                     int totalLinesProcessed) {
        long now = System.currentTimeMillis();
        long elapsed = now - startTime;

        // Calculate imbalance and std dev
        int imbalance = calculateImbalance();
        double stdDev = calculateStdDev();
        double throughput = elapsed > 0 ?
                (totalLinesProcessed * 1000.0 / elapsed) : 0;

        SystemSnapshot ss = new SystemSnapshot();
        ss.timestamp = now;
        ss.elapsedMs = elapsed;
        ss.completedChunks = completedChunks;
        ss.dispatchedChunks = dispatchedChunks;
        ss.busyWorkers = busyWorkers;
        ss.totalWorkers = totalWorkers;
        ss.imbalance = imbalance;
        ss.stdDev = stdDev;
        ss.throughput = throughput;
        systemSnapshots.add(ss);

        systemWriter.printf("%d,%d,%d,%d,%d,%d,%d,%.2f,%.2f%n",
                now, elapsed, completedChunks, dispatchedChunks,
                busyWorkers, totalWorkers, imbalance, stdDev, throughput);
        systemWriter.flush();
    }

    // =================== RL METRICS ===================

    /**
     * Record RL decision and update.
     */
    public void recordRLStep(int chunkId, String state, String action,
                             double reward, double epsilon,
                             double qBefore, double qAfter,
                             boolean wasExploration) {
        long now = System.currentTimeMillis();

        RLMetric rm = new RLMetric();
        rm.timestamp = now;
        rm.chunkId = chunkId;
        rm.state = state;
        rm.action = action;
        rm.reward = reward;
        rm.epsilon = epsilon;
        rm.qBefore = qBefore;
        rm.qAfter = qAfter;
        rm.wasExploration = wasExploration;
        rlMetrics.add(rm);

        rlWriter.printf("%d,%d,%s,%s,%.4f,%.4f,%.4f,%.4f,%s%n",
                now, chunkId, state.replace(",", ";"), action,
                reward, epsilon, qBefore, qAfter,
                wasExploration ? "EXPLORE" : "EXPLOIT");
        rlWriter.flush();
    }

    // =================== WORKER STATE ===================

    private void logWorkerState(String worker, String state) {
        WorkerMetrics wm = workerMetrics.get(worker);
        if (wm == null) return;

        long now = System.currentTimeMillis();
        workerStateWriter.printf("%d,%s,%s,%d,%.2f,%.2f,%d%n",
                now, worker, state,
                wm.chunksCompleted, wm.avgTime, wm.movingAvgTime,
                wm.totalIdleTimeMs);
        workerStateWriter.flush();
    }

    // =================== FAULT TOLERANCE ===================

    /**
     * Record worker failure.
     */
    public void recordWorkerFailure(String worker, String details) {
        long now = System.currentTimeMillis();

        FaultEvent fe = new FaultEvent();
        fe.timestamp = now;
        fe.eventType = "FAILURE";
        fe.worker = worker;
        fe.details = details;
        faultEvents.add(fe);

        faultWriter.printf("%d,%s,%s,%s,0,0,0%n",
                now, "FAILURE", worker, details.replace(",", ";"));
        faultWriter.flush();

        WorkerMetrics wm = workerMetrics.get(worker);
        if (wm != null) {
            wm.markFailed();
        }
    }

    /**
     * Record failure detection.
     */
    public void recordFailureDetected(String worker, long detectionTimeMs) {
        long now = System.currentTimeMillis();

        faultWriter.printf("%d,%s,%s,detected,%d,0,0%n",
                now, "DETECTION", worker, detectionTimeMs);
        faultWriter.flush();
    }

    /**
     * Record chunk redistribution after failure.
     */
    public void recordChunkRedistribution(String failedWorker, String newWorker,
                                          int chunkId, long recoveryTimeMs) {
        long now = System.currentTimeMillis();

        faultWriter.printf("%d,%s,%s,chunk_%d_to_%s,0,%d,1%n",
                now, "REDISTRIBUTION", failedWorker, chunkId, newWorker, recoveryTimeMs);
        faultWriter.flush();
    }

    /**
     * Record worker recovery.
     */
    public void recordWorkerRecovery(String worker, long downTimeMs) {
        long now = System.currentTimeMillis();

        faultWriter.printf("%d,%s,%s,recovered,0,%d,0%n",
                now, "RECOVERY", worker, downTimeMs);
        faultWriter.flush();

        WorkerMetrics wm = workerMetrics.get(worker);
        if (wm != null) {
            wm.markRecovered();
        }
    }

    // =================== CALCULATIONS ===================

    private int calculateImbalance() {
        if (workerMetrics.isEmpty()) return 0;

        int max = workerMetrics.values().stream()
                .filter(w -> !w.isFailed)
                .mapToInt(w -> w.chunksCompleted)
                .max().orElse(0);
        int min = workerMetrics.values().stream()
                .filter(w -> !w.isFailed)
                .mapToInt(w -> w.chunksCompleted)
                .min().orElse(0);

        return max - min;
    }

    private double calculateStdDev() {
        List<Integer> chunks = workerMetrics.values().stream()
                .filter(w -> !w.isFailed)
                .map(w -> w.chunksCompleted)
                .collect(Collectors.toList());  // <-- JAVA 11 СОВМЕСТИМО

        if (chunks.size() < 2) return 0;

        double avg = chunks.stream().mapToInt(Integer::intValue).average().orElse(0);
        double variance = chunks.stream()
                .mapToDouble(c -> Math.pow(c - avg, 2))
                .average()
                .orElse(0);

        return Math.sqrt(variance);
    }

    // =================== FINAL SUMMARY ===================

    /**
     * Write final summary and close all writers.
     */
    public void writeFinalSummary(long totalTimeMs, int totalChunks, int totalLines) {
        try {
            writeSummaryCSV(totalTimeMs, totalChunks, totalLines);
            writeWorkerSummary();
            writeDistributionAnalysis();

            closeWriters();

            System.out.println("✓ All metrics saved to: " + runDir.resolve("metrics"));

        } catch (IOException e) {
            System.err.println("Error writing final metrics: " + e.getMessage());
        }
    }

    private void writeSummaryCSV(long totalTimeMs, int totalChunks, int totalLines)
            throws IOException {
        try (PrintWriter w = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/summary.csv").toFile()))) {

            w.println("metric,value");
            w.println("mode," + mode);
            w.println("total_time_ms," + totalTimeMs);
            w.println("total_chunks," + totalChunks);
            w.println("total_lines," + totalLines);
            w.println("throughput_lines_per_sec," +
                    String.format("%.2f", totalLines * 1000.0 / totalTimeMs));
            w.println("throughput_chunks_per_sec," +
                    String.format("%.2f", totalChunks * 1000.0 / totalTimeMs));

            // Load balance
            w.println("final_imbalance," + calculateImbalance());
            w.println("final_std_dev," + String.format("%.2f", calculateStdDev()));

            // Worker counts
            long activeWorkers = workerMetrics.values().stream()
                    .filter(wm -> !wm.isFailed).count();
            long failedWorkersCount = workerMetrics.values().stream()
                    .filter(wm -> wm.isFailed).count();
            w.println("active_workers," + activeWorkers);
            w.println("failed_workers," + failedWorkersCount);

            // Fault tolerance
            long failureEvents = faultEvents.stream()
                    .filter(f -> f.eventType.equals("FAILURE")).count();
            w.println("failure_events," + failureEvents);
        }
    }

    private void writeWorkerSummary() throws IOException {
        try (PrintWriter w = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/workers_summary.csv").toFile()))) {

            w.println("worker,chunks,total_time_ms,avg_time_ms,moving_avg_ms," +
                    "min_time_ms,max_time_ms,idle_time_ms,failed");

            for (WorkerMetrics wm : workerMetrics.values()) {
                w.printf("%s,%d,%d,%.2f,%.2f,%d,%d,%d,%s%n",
                        wm.workerId,
                        wm.chunksCompleted,
                        wm.totalProcessingTime,
                        wm.avgTime,
                        wm.movingAvgTime,
                        wm.minTime,
                        wm.maxTime,
                        wm.totalIdleTimeMs,
                        wm.isFailed);
            }
        }
    }

    private void writeDistributionAnalysis() throws IOException {
        try (PrintWriter w = new PrintWriter(new FileWriter(
                runDir.resolve("metrics/distribution.csv").toFile()))) {

            w.println("worker,chunks,percentage,category");

            int totalChunks = workerMetrics.values().stream()
                    .mapToInt(wm -> wm.chunksCompleted).sum();

            for (WorkerMetrics wm : workerMetrics.values()) {
                double pct = totalChunks > 0 ?
                        (wm.chunksCompleted * 100.0 / totalChunks) : 0;

                String category;
                if (wm.avgTime < 200) category = "FAST";
                else if (wm.avgTime < 400) category = "MEDIUM";
                else category = "SLOW";

                w.printf("%s,%d,%.2f,%s%n",
                        wm.workerId, wm.chunksCompleted, pct, category);
            }
        }
    }

    private void closeWriters() {
        if (chunkWriter != null) chunkWriter.close();
        if (systemWriter != null) systemWriter.close();
        if (rlWriter != null) rlWriter.close();
        if (workerStateWriter != null) workerStateWriter.close();
        if (faultWriter != null) faultWriter.close();
    }

    // =================== INNER CLASSES ===================

    private static class WorkerMetrics {
        final String workerId;
        int chunksCompleted = 0;
        long totalProcessingTime = 0;
        double avgTime = 0;
        double movingAvgTime = 0;
        long minTime = Long.MAX_VALUE;
        long maxTime = 0;
        boolean isBusy = false;
        boolean isFailed = false;
        long lastBusyStart = 0;
        long totalIdleTimeMs = 0;
        long lastIdleStart;

        // Moving average window
        private final LinkedList<Long> recentTimes = new LinkedList<>();
        private static final int WINDOW_SIZE = 10;

        WorkerMetrics(String workerId) {
            this.workerId = workerId;
            this.lastIdleStart = System.currentTimeMillis();
        }

        void markBusy() {
            if (!isBusy) {
                totalIdleTimeMs += System.currentTimeMillis() - lastIdleStart;
                lastBusyStart = System.currentTimeMillis();
                isBusy = true;
            }
        }

        void recordChunkCompletion(long timeMs, long memBefore, long memAfter) {
            chunksCompleted++;
            totalProcessingTime += timeMs;
            avgTime = (double) totalProcessingTime / chunksCompleted;

            minTime = Math.min(minTime, timeMs);
            maxTime = Math.max(maxTime, timeMs);

            // Update moving average
            recentTimes.addLast(timeMs);
            if (recentTimes.size() > WINDOW_SIZE) {
                recentTimes.removeFirst();
            }
            movingAvgTime = recentTimes.stream()
                    .mapToLong(Long::longValue)
                    .average()
                    .orElse(0);

            isBusy = false;
            lastIdleStart = System.currentTimeMillis();
        }

        void markFailed() {
            isFailed = true;
            isBusy = false;
        }

        void markRecovered() {
            isFailed = false;
            lastIdleStart = System.currentTimeMillis();
        }
    }

    private static class ChunkMetric {
        long timestamp;
        int chunkId;
        String worker;
        int lines;
        long processingTimeMs;
        long memBeforeMB;
        long memAfterMB;
        int systemImbalance;
    }

    private static class SystemSnapshot {
        long timestamp;
        long elapsedMs;
        int completedChunks;
        int dispatchedChunks;
        int busyWorkers;
        int totalWorkers;
        int imbalance;
        double stdDev;
        double throughput;
    }

    private static class RLMetric {
        long timestamp;
        int chunkId;
        String state;
        String action;
        double reward;
        double epsilon;
        double qBefore;
        double qAfter;
        boolean wasExploration;
    }

    private static class FaultEvent {
        long timestamp;
        String eventType;
        String worker;
        String details;
    }
}