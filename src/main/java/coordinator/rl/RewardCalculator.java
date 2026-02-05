package coordinator.rl;

import java.util.*;

/**
 * ИСПРАВЛЕННЫЙ Reward Calculator для RL-based scheduling.
 *
 * КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
 * 1. Reward немедленный — не ждём накопления статистики
 * 2. Reward каузальный — зависит только от последнего действия
 * 3. Reward нормализован — в диапазоне [-1, +1]
 * 4. Reward информативен с первого чанка
 *
 * Компоненты reward:
 * - Speed component: насколько быстро воркер обработал чанк относительно других
 * - Balance component: как решение повлияло на баланс нагрузки
 * - Efficiency component: выбрали ли мы "правильного" воркера для текущей ситуации
 *
 * @author Expert Review
 */
public class RewardCalculator {

    // Веса компонентов (должны суммироваться в 1.0)
    private static final double SPEED_WEIGHT = 0.6;
    private static final double BALANCE_WEIGHT = 0.3;
    private static final double EFFICIENCY_WEIGHT = 0.1;

    // Для нормализации времени
    private double globalMinTime = Double.MAX_VALUE;
    private double globalMaxTime = 0;

    // История для адаптивной нормализации
    private final List<Double> recentTimes = new ArrayList<>();
    private static final int HISTORY_SIZE = 50;

    /**
     * Вычисляет reward для последнего действия.
     *
     * @param processingTimeMs время обработки чанка выбранным воркером
     * @param selectedWorker ID выбранного воркера
     * @param allWorkerTimes среднее время всех воркеров (для сравнения)
     * @param imbalanceBefore дисбаланс до назначения
     * @param imbalanceAfter дисбаланс после назначения
     * @param wasOptimalChoice был ли это оптимальный выбор (наименее загруженный быстрый воркер)
     * @return reward в диапазоне [-1, +1]
     */
    public double compute(long processingTimeMs,
                          String selectedWorker,
                          Map<String, Double> allWorkerTimes,
                          int imbalanceBefore,
                          int imbalanceAfter,
                          boolean wasOptimalChoice) {

        // Update normalization bounds
        updateNormalizationBounds(processingTimeMs);

        // === Component 1: Speed Reward ===
        double speedReward = computeSpeedReward(processingTimeMs, allWorkerTimes);

        // === Component 2: Balance Reward ===
        double balanceReward = computeBalanceReward(imbalanceBefore, imbalanceAfter);

        // === Component 3: Efficiency Reward ===
        double efficiencyReward = wasOptimalChoice ? 1.0 : -0.5;

        // === Combined Reward ===
        double reward = SPEED_WEIGHT * speedReward +
                BALANCE_WEIGHT * balanceReward +
                EFFICIENCY_WEIGHT * efficiencyReward;

        // Clip to [-1, +1]
        return Math.max(-1.0, Math.min(1.0, reward));
    }

    /**
     * Упрощённый compute для обратной совместимости.
     */
    public double compute(long processingTimeMs,
                          String selectedWorker,
                          Map<String, WorkerStats> workerStats,
                          int imbalanceAfter) {

        Map<String, Double> avgTimes = new HashMap<>();
        for (Map.Entry<String, WorkerStats> entry : workerStats.entrySet()) {
            avgTimes.put(entry.getKey(), entry.getValue().avgTime);
        }

        // Estimate imbalanceBefore (approximate)
        int imbalanceBefore = Math.max(0, imbalanceAfter - 1);

        // Determine if optimal (simplified: fastest available worker)
        boolean wasOptimal = isOptimalChoice(selectedWorker, workerStats);

        return compute(processingTimeMs, selectedWorker, avgTimes,
                imbalanceBefore, imbalanceAfter, wasOptimal);
    }

    /**
     * Speed reward: сравниваем с медианным временем системы.
     * Используем медиану вместо среднего для устойчивости к выбросам.
     */
    private double computeSpeedReward(long processingTimeMs, Map<String, Double> allWorkerTimes) {
        if (allWorkerTimes.isEmpty()) {
            // Первый чанк — neutral reward
            return 0.0;
        }

        // Compute median processing time
        List<Double> times = new ArrayList<>(allWorkerTimes.values());
        times.removeIf(t -> t <= 0);

        if (times.isEmpty()) {
            return 0.0;
        }

        Collections.sort(times);
        double median = times.get(times.size() / 2);

        if (median <= 0) {
            return 0.0;
        }

        // Reward = (median - actual) / median
        // Positive if faster than median, negative if slower
        double improvement = (median - processingTimeMs) / median;

        // Scale to [-1, +1]
        return Math.max(-1.0, Math.min(1.0, improvement));
    }

    /**
     * Balance reward: штраф за увеличение дисбаланса.
     */
    private double computeBalanceReward(int imbalanceBefore, int imbalanceAfter) {
        int delta = imbalanceAfter - imbalanceBefore;

        if (delta <= 0) {
            // Balance improved or stayed same
            return 0.5;
        } else if (delta == 1) {
            // Minor increase (expected when assigning)
            return 0.0;
        } else {
            // Significant increase
            return -0.5;
        }
    }

    /**
     * Определяет, был ли выбор оптимальным.
     * Оптимальный = наименее загруженный среди быстрых воркеров.
     */
    private boolean isOptimalChoice(String selectedWorker,
                                    Map<String, WorkerStats> workerStats) {
        if (workerStats.isEmpty()) {
            return true; // No data = any choice is fine
        }

        WorkerStats selected = workerStats.get(selectedWorker);
        if (selected == null) {
            return true;
        }

        // Find workers faster than selected
        long fasterAndLessLoaded = workerStats.entrySet().stream()
                .filter(e -> !e.getKey().equals(selectedWorker))
                .filter(e -> e.getValue().avgTime > 0)
                .filter(e -> e.getValue().avgTime < selected.avgTime * 0.8) // 20% faster
                .filter(e -> e.getValue().chunksCompleted <= selected.chunksCompleted)
                .count();

        return fasterAndLessLoaded == 0;
    }

    /**
     * Обновляет границы для нормализации.
     */
    private void updateNormalizationBounds(long processingTimeMs) {
        recentTimes.add((double) processingTimeMs);
        if (recentTimes.size() > HISTORY_SIZE) {
            recentTimes.remove(0);
        }

        globalMinTime = Math.min(globalMinTime, processingTimeMs);
        globalMaxTime = Math.max(globalMaxTime, processingTimeMs);
    }

    /**
     * Вычисляет дисбаланс нагрузки.
     */
    public int calculateImbalance(Map<String, Integer> chunksPerWorker) {
        if (chunksPerWorker.isEmpty()) return 0;

        Collection<Integer> chunks = chunksPerWorker.values();
        return Collections.max(chunks) - Collections.min(chunks);
    }

    /**
     * Вычисляет стандартное отклонение распределения чанков.
     */
    public double calculateStdDev(Map<String, Integer> chunksPerWorker) {
        if (chunksPerWorker.isEmpty()) return 0;

        double avg = chunksPerWorker.values().stream()
                .mapToInt(Integer::intValue)
                .average()
                .orElse(0);

        double variance = chunksPerWorker.values().stream()
                .mapToDouble(c -> Math.pow(c - avg, 2))
                .average()
                .orElse(0);

        return Math.sqrt(variance);
    }

    // === Statistics for logging ===
    public double getGlobalMinTime() { return globalMinTime; }
    public double getGlobalMaxTime() { return globalMaxTime; }

    /**
     * Worker statistics container.
     */
    public static class WorkerStats {
        public double avgTime;
        public int chunksCompleted;
        public boolean isBusy;
        public long lastChunkTime;

        public WorkerStats(double avgTime, int chunksCompleted) {
            this.avgTime = avgTime;
            this.chunksCompleted = chunksCompleted;
            this.isBusy = false;
            this.lastChunkTime = 0;
        }

        public WorkerStats(double avgTime, int chunksCompleted, boolean isBusy, long lastChunkTime) {
            this.avgTime = avgTime;
            this.chunksCompleted = chunksCompleted;
            this.isBusy = isBusy;
            this.lastChunkTime = lastChunkTime;
        }

        @Override
        public String toString() {
            return String.format("Stats[avg=%.1fms, chunks=%d, busy=%s]",
                    avgTime, chunksCompleted, isBusy);
        }
    }
}
