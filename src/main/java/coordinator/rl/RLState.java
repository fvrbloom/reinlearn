package coordinator.rl;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ИСПРАВЛЕННЫЙ RL State для гетерогенного MapReduce scheduling.
 *
 * КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
 * 1. State описывает СИСТЕМУ, а не конкретного воркера
 * 2. State стабилен и детерминирован
 * 3. State позволяет RL обобщать опыт
 *
 * State Space Size: 3 × 3 × 3 × 4 = 108 состояний (управляемо)
 *
 * @author Expert Review
 */
public class RLState implements Serializable {

    private static final long serialVersionUID = 4L;

    // === Dimension 1: Global Load Balance ===
    public enum LoadBalance {
        BALANCED,      // max-min <= 2 chunks
        MODERATE,      // max-min 3-5 chunks
        IMBALANCED     // max-min > 5 chunks
    }

    // === Dimension 2: System Utilization ===
    public enum Utilization {
        LOW,           // < 30% workers busy
        MEDIUM,        // 30-70% workers busy
        HIGH           // > 70% workers busy
    }

    // === Dimension 3: Performance Variance ===
    public enum PerformanceVariance {
        HOMOGENEOUS,   // CV < 0.2 (workers similar speed)
        MODERATE,      // CV 0.2-0.5
        HETEROGENEOUS  // CV > 0.5 (workers very different)
    }

    // === Dimension 4: Progress Phase ===
    public enum ProgressPhase {
        EARLY,         // < 25% chunks done
        MIDDLE,        // 25-75% chunks done
        LATE,          // > 75% chunks done
        FINAL          // > 90% chunks done
    }

    private final LoadBalance loadBalance;
    private final Utilization utilization;
    private final PerformanceVariance performanceVariance;
    private final ProgressPhase progressPhase;

    /**
     * Конструктор состояния из метрик системы.
     *
     * @param chunksPerWorker количество чанков на каждом воркере
     * @param avgTimesPerWorker среднее время обработки каждого воркера (ms)
     * @param busyWorkers множество занятых воркеров
     * @param totalWorkers общее количество воркеров
     * @param completedChunks сколько чанков уже выполнено
     * @param estimatedTotalChunks оценка общего числа чанков
     */
    public RLState(Map<String, Integer> chunksPerWorker,
                   Map<String, Double> avgTimesPerWorker,
                   Set<String> busyWorkers,
                   int totalWorkers,
                   int completedChunks,
                   int estimatedTotalChunks) {


        // === 1. LOAD BALANCE ===
        this.loadBalance = computeLoadBalance(chunksPerWorker);

        // === 2. UTILIZATION ===
        this.utilization = computeUtilization(busyWorkers.size(), totalWorkers);

        // === 3. PERFORMANCE VARIANCE ===
        this.performanceVariance = computePerformanceVariance(avgTimesPerWorker);

        // === 4. PROGRESS PHASE ===
        this.progressPhase = computeProgressPhase(completedChunks, estimatedTotalChunks);
    }

    private LoadBalance computeLoadBalance(Map<String, Integer> chunksPerWorker) {
        if (chunksPerWorker.isEmpty() || chunksPerWorker.size() == 1) {
            return LoadBalance.BALANCED;
        }

        Collection<Integer> chunks = chunksPerWorker.values();
        int max = Collections.max(chunks);
        int min = Collections.min(chunks);
        int diff = max - min;

        if (diff <= 2) return LoadBalance.BALANCED;
        if (diff <= 5) return LoadBalance.MODERATE;
        return LoadBalance.IMBALANCED;
    }

    private Utilization computeUtilization(int busyCount, int totalWorkers) {
        if (totalWorkers == 0) return Utilization.LOW;

        double ratio = (double) busyCount / totalWorkers;

        if (ratio < 0.3) return Utilization.LOW;
        if (ratio <= 0.7) return Utilization.MEDIUM;
        return Utilization.HIGH;
    }

    private PerformanceVariance computePerformanceVariance(Map<String, Double> avgTimes) {
        if (avgTimes.isEmpty() || avgTimes.size() == 1) {
            return PerformanceVariance.HOMOGENEOUS;
        }

        List<Double> times = avgTimes.values().stream()
                .filter(t -> t > 0)
                .collect(Collectors.toList());

        if (times.size() < 2) {
            return PerformanceVariance.HOMOGENEOUS;
        }

        double mean = times.stream().mapToDouble(d -> d).average().orElse(0);
        if (mean == 0) return PerformanceVariance.HOMOGENEOUS;

        double variance = times.stream()
                .mapToDouble(t -> Math.pow(t - mean, 2))
                .average()
                .orElse(0);

        double stdDev = Math.sqrt(variance);
        double cv = stdDev / mean; // Coefficient of Variation

        if (cv < 0.2) return PerformanceVariance.HOMOGENEOUS;
        if (cv <= 0.5) return PerformanceVariance.MODERATE;
        return PerformanceVariance.HETEROGENEOUS;
    }

    private ProgressPhase computeProgressPhase(int completed, int estimated) {
        if (estimated <= 0) return ProgressPhase.EARLY;

        double progress = (double) completed / estimated;

        if (progress < 0.25) return ProgressPhase.EARLY;
        if (progress < 0.75) return ProgressPhase.MIDDLE;
        if (progress < 0.90) return ProgressPhase.LATE;
        return ProgressPhase.FINAL;
    }

    // === Equality for HashMap key ===
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RLState)) return false;
        RLState that = (RLState) o;
        return loadBalance == that.loadBalance &&
                utilization == that.utilization &&
                performanceVariance == that.performanceVariance &&
                progressPhase == that.progressPhase;
    }

    @Override
    public int hashCode() {
        return Objects.hash(loadBalance, utilization, performanceVariance, progressPhase);
    }

    @Override
    public String toString() {
        return String.format("S[%s,%s,%s,%s]",
                loadBalance, utilization, performanceVariance, progressPhase);
    }

    // === Getters for analysis ===
    public LoadBalance getLoadBalance() { return loadBalance; }
    public Utilization getUtilization() { return utilization; }
    public PerformanceVariance getPerformanceVariance() { return performanceVariance; }
    public ProgressPhase getProgressPhase() { return progressPhase; }

    /**
     * Возвращает числовой вектор состояния для анализа.
     */
    public int[] toVector() {
        return new int[] {
                loadBalance.ordinal(),
                utilization.ordinal(),
                performanceVariance.ordinal(),
                progressPhase.ordinal()
        };
    }

    /**
     * Создаёт начальное состояние.
     */
    public static RLState initial(int totalWorkers, int estimatedTotalChunks) {
        return new RLState(
                Collections.emptyMap(),
                Collections.emptyMap(),
                Collections.emptySet(),
                totalWorkers,
                0,
                estimatedTotalChunks
        );
    }
}
