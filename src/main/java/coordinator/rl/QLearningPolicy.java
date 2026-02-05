package coordinator.rl;

import java.util.*;
import java.io.*;

/**
 * ИСПРАВЛЕННЫЙ Q-Learning Policy для worker selection.
 *
 * КРИТИЧЕСКИЕ ИСПРАВЛЕНИЯ:
 * 1. Правильное использование nextState в Q-update
 * 2. Action space = множество воркеров (не привязано к state)
 * 3. UCB-based exploration вместо простого epsilon-greedy
 * 4. Полное логирование для анализа обучения
 *
 * @author Expert Review
 */
public class QLearningPolicy {

    private final Map<RLState, Map<String, Double>> qTable = new HashMap<>();
    private final Map<RLState, Map<String, Integer>> visitCount = new HashMap<>(); // Для UCB

    private final double alpha;        // Learning rate
    private final double gamma;        // Discount factor
    private double epsilon;            // Exploration rate
    private final double epsilonMin;
    private final double epsilonDecay;

    // UCB exploration parameter
    private static final double UCB_C = 1.5;

    private final Random random = new Random(42); // Reproducible

    // Statistics
    private int totalUpdates = 0;
    private int explorationCount = 0;
    private int exploitationCount = 0;
    private double totalReward = 0.0;

    // Detailed logging
    private final List<LearningStep> learningHistory = new ArrayList<>();
    private static final int MAX_HISTORY = 10000;

    public QLearningPolicy(double alpha, double gamma, double epsilon,
                           double epsilonMin, double epsilonDecay) {
        this.alpha = alpha;
        this.gamma = gamma;
        this.epsilon = epsilon;
        this.epsilonMin = epsilonMin;
        this.epsilonDecay = epsilonDecay;
    }

    /**
     * Select worker using epsilon-greedy with UCB bonus for exploration.
     *
     * @param state Current system state
     * @param availableWorkers Workers that can accept chunks (not busy)
     * @return Selected worker ID
     */
    public String selectAction(RLState state, Set<String> availableWorkers) {
        if (availableWorkers.isEmpty()) {
            throw new IllegalArgumentException("No available workers");
        }

        if (availableWorkers.size() == 1) {
            return availableWorkers.iterator().next();
        }

        ensureStateExists(state, availableWorkers);

        // Epsilon-greedy with UCB exploration bonus
        if (random.nextDouble() < epsilon) {
            // EXPLORATION: UCB-based selection (not purely random)
            explorationCount++;
            return selectUCB(state, availableWorkers);
        } else {
            // EXPLOITATION: Best Q-value
            exploitationCount++;
            return selectBestQValue(state, availableWorkers);
        }
    }

    /**
     * UCB-based exploration: prefer less-visited actions.
     * UCB = Q(s,a) + c * sqrt(ln(N(s)) / N(s,a))
     */
    private String selectUCB(RLState state, Set<String> availableWorkers) {
        Map<String, Double> qValues = qTable.get(state);
        Map<String, Integer> visits = visitCount.get(state);

        int totalVisits = visits.values().stream().mapToInt(Integer::intValue).sum();
        if (totalVisits == 0) {
            // No visits yet - random selection
            return randomWorker(availableWorkers);
        }

        String bestWorker = null;
        double bestUCB = Double.NEGATIVE_INFINITY;

        for (String worker : availableWorkers) {
            double q = qValues.getOrDefault(worker, 0.0);
            int n = visits.getOrDefault(worker, 0);

            double ucb;
            if (n == 0) {
                ucb = Double.MAX_VALUE; // Never visited = highest priority
            } else {
                ucb = q + UCB_C * Math.sqrt(Math.log(totalVisits + 1) / n);
            }

            if (ucb > bestUCB) {
                bestUCB = ucb;
                bestWorker = worker;
            }
        }

        return bestWorker != null ? bestWorker : randomWorker(availableWorkers);
    }

    /**
     * Select worker with highest Q-value.
     */
    private String selectBestQValue(RLState state, Set<String> availableWorkers) {
        Map<String, Double> qValues = qTable.get(state);

        return availableWorkers.stream()
                .max(Comparator.comparing(w -> qValues.getOrDefault(w, 0.0)))
                .orElseThrow();
    }

    /**
     * ИСПРАВЛЕННЫЙ Q-learning update с правильным nextState.
     *
     * Q(s,a) ← Q(s,a) + α[r + γ max_a' Q(s',a') - Q(s,a)]
     *
     * @param state Current state when action was taken
     * @param action Worker that was selected
     * @param reward Observed reward
     * @param nextState State after action (REQUIRED, not null!)
     * @param allWorkers All workers for nextState Q-value lookup
     */
    public void update(RLState state, String action, double reward,
                       RLState nextState, Set<String> allWorkers) {

        if (state == null || action == null) {
            System.err.println("WARNING: Null state or action in Q-update, skipping");
            return;
        }

        ensureStateExists(state, allWorkers);
        incrementVisitCount(state, action);

        double currentQ = qTable.get(state).getOrDefault(action, 0.0);

        // Compute max Q(s', a') for next state
        double maxNextQ = 0.0;
        if (nextState != null) {
            ensureStateExists(nextState, allWorkers);
            maxNextQ = qTable.get(nextState).values().stream()
                    .max(Double::compare)
                    .orElse(0.0);
        }
        // If nextState is null (terminal), maxNextQ = 0 (correct for terminal states)

        // Bellman equation update
        double newQ = currentQ + alpha * (reward + gamma * maxNextQ - currentQ);
        qTable.get(state).put(action, newQ);

        // Statistics
        totalUpdates++;
        totalReward += reward;

        // Decay epsilon
        epsilon = Math.max(epsilonMin, epsilon * epsilonDecay);

        // Log learning step
        logLearningStep(state, action, reward, nextState, currentQ, newQ);

        // Periodic debug output
        if (totalUpdates % 100 == 0) {
            printLearningProgress();
        }
    }

    /**
     * Initialize Q-values and visit counts for new state.
     */
    private void ensureStateExists(RLState state, Set<String> workers) {
        qTable.computeIfAbsent(state, k -> new HashMap<>());
        visitCount.computeIfAbsent(state, k -> new HashMap<>());

        Map<String, Double> qValues = qTable.get(state);
        Map<String, Integer> visits = visitCount.get(state);

        for (String worker : workers) {
            qValues.putIfAbsent(worker, 0.0);
            visits.putIfAbsent(worker, 0);
        }
    }

    private void incrementVisitCount(RLState state, String action) {
        visitCount.get(state).merge(action, 1, Integer::sum);
    }

    private String randomWorker(Set<String> workers) {
        int index = random.nextInt(workers.size());
        return workers.stream().skip(index).findFirst().orElseThrow();
    }

    // =================== LOGGING ===================

    private void logLearningStep(RLState state, String action, double reward,
                                 RLState nextState, double oldQ, double newQ) {
        if (learningHistory.size() >= MAX_HISTORY) {
            learningHistory.remove(0);
        }
        learningHistory.add(new LearningStep(
                totalUpdates, state, action, reward, nextState, oldQ, newQ, epsilon
        ));
    }

    private void printLearningProgress() {
        double avgReward = totalReward / totalUpdates;
        double recentAvgReward = getRecentAverageReward(100);

        System.out.printf("  [RL] Updates=%d | ε=%.4f | AvgR=%.3f | Recent100R=%.3f | States=%d%n",
                totalUpdates, epsilon, avgReward, recentAvgReward, qTable.size());
    }

    public double getRecentAverageReward(int n) {
        if (learningHistory.isEmpty()) return 0.0;

        int start = Math.max(0, learningHistory.size() - n);
        return learningHistory.subList(start, learningHistory.size()).stream()
                .mapToDouble(s -> s.reward)
                .average()
                .orElse(0.0);
    }

    // =================== ANALYSIS ===================

    /**
     * Get best action for a state (no exploration).
     */
    public String getBestAction(RLState state, Set<String> availableWorkers) {
        if (!qTable.containsKey(state)) {
            return randomWorker(availableWorkers);
        }
        return selectBestQValue(state, availableWorkers);
    }

    /**
     * Get Q-value for state-action pair.
     */
    public double getQValue(RLState state, String action) {
        return qTable.getOrDefault(state, Collections.emptyMap())
                .getOrDefault(action, 0.0);
    }

    /**
     * Print top Q-values for analysis.
     */
    public void printTopQValues(int topN) {
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  TOP Q-VALUES (Learned Policies)                 ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        List<QEntry> entries = new ArrayList<>();
        for (Map.Entry<RLState, Map<String, Double>> stateEntry : qTable.entrySet()) {
            RLState state = stateEntry.getKey();
            for (Map.Entry<String, Double> actionEntry : stateEntry.getValue().entrySet()) {
                double q = actionEntry.getValue();
                if (Math.abs(q) > 0.001) { // Skip near-zero entries
                    entries.add(new QEntry(state, actionEntry.getKey(), q));
                }
            }
        }

        entries.stream()
                .sorted((a, b) -> Double.compare(Math.abs(b.qValue), Math.abs(a.qValue)))
                .limit(topN)
                .forEach(e -> {
                    String sign = e.qValue >= 0 ? "+" : "";
                    System.out.printf("  Q=%s%.4f | %s → %s%n",
                            sign, e.qValue, e.state, e.action);
                });

        System.out.println();
    }

    /**
     * Print policy summary: best action for each visited state.
     */
    public void printPolicySummary() {
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  LEARNED POLICY SUMMARY                          ║");
        System.out.println("╚══════════════════════════════════════════════════╝");

        for (Map.Entry<RLState, Map<String, Double>> entry : qTable.entrySet()) {
            RLState state = entry.getKey();
            Map<String, Double> actions = entry.getValue();

            String bestAction = actions.entrySet().stream()
                    .max(Comparator.comparing(Map.Entry::getValue))
                    .map(Map.Entry::getKey)
                    .orElse("none");

            double bestQ = actions.getOrDefault(bestAction, 0.0);

            int totalVisits = visitCount.getOrDefault(state, Collections.emptyMap())
                    .values().stream().mapToInt(Integer::intValue).sum();

            System.out.printf("  %s → %s (Q=%.3f, visits=%d)%n",
                    state, bestAction, bestQ, totalVisits);
        }
        System.out.println();
    }

    // =================== PERSISTENCE ===================

    public void saveToFile(String path) {
        try {
            File file = new File(path);
            if (file.getParentFile() != null) {
                file.getParentFile().mkdirs();
            }

            try (ObjectOutputStream out = new ObjectOutputStream(
                    new FileOutputStream(file))) {
                out.writeObject(qTable);
                out.writeObject(visitCount);
                out.writeDouble(epsilon);
                out.writeInt(totalUpdates);
                out.writeDouble(totalReward);
                out.writeInt(explorationCount);
                out.writeInt(exploitationCount);
            }

            System.out.printf("✓ Q-table saved: %d states, %d updates, ε=%.4f%n",
                    qTable.size(), totalUpdates, epsilon);

        } catch (IOException e) {
            System.err.println("Failed to save Q-table: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public void loadFromFile(String path) {
        File file = new File(path);
        if (!file.exists()) {
            System.out.println("ℹ Starting with empty Q-table");
            return;
        }

        try (ObjectInputStream in = new ObjectInputStream(
                new FileInputStream(file))) {

            Map<RLState, Map<String, Double>> loadedQ =
                    (Map<RLState, Map<String, Double>>) in.readObject();

            Map<RLState, Map<String, Integer>> loadedVisits;
            try {
                loadedVisits = (Map<RLState, Map<String, Integer>>) in.readObject();
            } catch (Exception e) {
                loadedVisits = new HashMap<>();
            }

            double loadedEpsilon = in.readDouble();
            int loadedUpdates = in.readInt();
            double loadedReward = in.readDouble();

            int loadedExploration = 0, loadedExploitation = 0;
            try {
                loadedExploration = in.readInt();
                loadedExploitation = in.readInt();
            } catch (Exception ignored) {}

            qTable.clear();
            qTable.putAll(loadedQ);
            visitCount.clear();
            visitCount.putAll(loadedVisits);
            epsilon = loadedEpsilon;
            totalUpdates = loadedUpdates;
            totalReward = loadedReward;
            explorationCount = loadedExploration;
            exploitationCount = loadedExploitation;

            System.out.printf("✓ Q-table loaded: %d states, %d updates, ε=%.4f%n",
                    qTable.size(), totalUpdates, epsilon);

        } catch (Exception e) {
            System.out.println("⚠ Cannot load Q-table: " + e.getMessage());
        }
    }

    // =================== STATISTICS ===================

    public void printStatistics() {
        int totalActions = explorationCount + exploitationCount;
        double exploreRatio = totalActions > 0 ?
                (double) explorationCount / totalActions : 0.0;
        double avgReward = totalUpdates > 0 ? totalReward / totalUpdates : 0.0;

        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  Q-LEARNING STATISTICS                           ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("  States explored:      " + qTable.size());
        System.out.println("  Total updates:        " + totalUpdates);
        System.out.println("  Exploration actions:  " + explorationCount);
        System.out.println("  Exploitation actions: " + exploitationCount);
        System.out.printf("  Exploration ratio:    %.2f%%%n", exploreRatio * 100);
        System.out.printf("  Current epsilon:      %.4f%n", epsilon);
        System.out.printf("  Average reward:       %.4f%n", avgReward);
        System.out.printf("  Recent reward (100):  %.4f%n", getRecentAverageReward(100));
        System.out.println("══════════════════════════════════════════════════\n");
    }

    /**
     * Export learning history to CSV for analysis.
     */
    public void exportLearningHistory(String path) {
        try (PrintWriter writer = new PrintWriter(new FileWriter(path))) {
            writer.println("step,state,action,reward,old_q,new_q,epsilon");

            for (LearningStep step : learningHistory) {
                writer.printf("%d,%s,%s,%.4f,%.4f,%.4f,%.4f%n",
                        step.stepNumber,
                        step.state.toString().replace(",", ";"),
                        step.action,
                        step.reward,
                        step.oldQ,
                        step.newQ,
                        step.epsilon);
            }

            System.out.println("✓ Learning history exported: " + path);
        } catch (IOException e) {
            System.err.println("Failed to export history: " + e.getMessage());
        }
    }

    // Getters
    public double getEpsilon() { return epsilon; }
    public int getTotalUpdates() { return totalUpdates; }
    public int getStateSpaceSize() { return qTable.size(); }
    public double getAverageReward() {
        return totalUpdates > 0 ? totalReward / totalUpdates : 0.0;
    }
    public double getRecentAverageReward() { return getRecentAverageReward(100); }

    // =================== INNER CLASSES ===================

    private static class QEntry {
        final RLState state;
        final String action;
        final double qValue;

        QEntry(RLState state, String action, double qValue) {
            this.state = state;
            this.action = action;
            this.qValue = qValue;
        }
    }

    private static class LearningStep implements Serializable {
        final int stepNumber;
        final RLState state;
        final String action;
        final double reward;
        final RLState nextState;
        final double oldQ;
        final double newQ;
        final double epsilon;

        LearningStep(int stepNumber, RLState state, String action, double reward,
                     RLState nextState, double oldQ, double newQ, double epsilon) {
            this.stepNumber = stepNumber;
            this.state = state;
            this.action = action;
            this.reward = reward;
            this.nextState = nextState;
            this.oldQ = oldQ;
            this.newQ = newQ;
            this.epsilon = epsilon;
        }
    }
}
