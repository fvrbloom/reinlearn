package coordinator;
import java.util.stream.Collectors;

import jade.core.Agent;
import jade.core.AID;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import coordinator.rl.*;
import coordinator.metrics.MetricsCollector;
import coordinator.hdfs.HDFSDataSource;
import coordinator.hdfs.HDFSDataSource.ChunkIterator;
import coordinator.hdfs.HDFSDataSource.ChunkData;
import coordinator.hdfs.HDFSDataSource.BlockInfo;

/**
 * Master Agent с ОБЯЗАТЕЛЬНОЙ интеграцией HDFS.
 *
 * АРХИТЕКТУРА:
 * ============
 * AWS EC2 (гетерогенные узлы)
 *   → HDFS (распределённое хранение данных)  <-- ОБЯЗАТЕЛЬНО
 *   → JADE (multi-agent система)
 *   → Собственная реализация MapReduce
 *   → Протокол распределения (RL / RoundRobin / LeastLoaded)
 *
 * КОНФИГУРАЦИЯ HDFS:
 * ==================
 * Передать через аргументы агента:
 *   master:coordinator.MasterAgent(RL,4,hdfs://10.0.0.1:9000,/data/big6.txt)
 *
 * @author Expert Review - Thesis Version
 */
public class MasterAgent extends Agent {

    // =================== CONFIGURATION ===================
    public enum SchedulingMode {
        RL, ROUND_ROBIN, LEAST_LOADED
    }

    private SchedulingMode currentMode = SchedulingMode.RL;
    private int expectedWorkers = 1;
    private int maxChunkLines = 5000;

    // HDFS Configuration (ОБЯЗАТЕЛЬНО)
    private String hdfsUri = "hdfs://172.31.68.222:9000";  // Изменить на реальный IP Master'а
    private String hdfsInputPath = "/input/big6.txt";

    private static final String QTABLE_PATH =
            System.getProperty("user.home") + "/results/models/rl_qtable.ser";

    // =================== HDFS ===================
    private HDFSDataSource hdfsDataSource;
    private ChunkIterator chunkIterator;
    private List<BlockInfo> blockLocations;  // Информация о блоках для data locality

    // =================== FAULT TOLERANCE ===================
    private static final long HEARTBEAT_TIMEOUT_MS = 10000;
    private static final long HEARTBEAT_CHECK_INTERVAL_MS = 5000;

    // =================== RL COMPONENTS ===================
    private QLearningPolicy rlPolicy;
    private RewardCalculator rewardCalculator;
    private RLState lastState = null;
    private String lastAction = null;
    private int lastChunkId = 0;
    private double lastQValue = 0.0;

    // =================== WORKER STATE ===================
    private final Set<String> aliveWorkers = ConcurrentHashMap.newKeySet();
    private final Set<String> failedWorkers = ConcurrentHashMap.newKeySet();
    private final Map<String, Boolean> workerBusy = new ConcurrentHashMap<>();
    private final Map<String, Integer> workerChunkCount = new ConcurrentHashMap<>();
    private final Map<String, Long> workerTotalTime = new ConcurrentHashMap<>();
    private final Map<String, Double> workerAvgTime = new ConcurrentHashMap<>();
    private final Map<String, Long> workerLastHeartbeat = new ConcurrentHashMap<>();
    private final Map<String, Long> workerLastFinishTime = new ConcurrentHashMap<>();
    private final Map<String, Integer> workerCores = new ConcurrentHashMap<>();
    private final Map<String, Long> workerMaxMemory = new ConcurrentHashMap<>();
    private final Map<String, String> workerHostname = new ConcurrentHashMap<>();  // Для data locality
    private final Map<String, List<PendingChunk>> workerPendingChunks = new ConcurrentHashMap<>();

    // =================== MAPREDUCE STATE ===================
    private boolean eofReached = false;
    private boolean mapReduceStarted = false;
    private long globalStartTime;
    private int totalLinesProcessed = 0;
    private int totalChunksDispatched = 0;
    private int totalChunksCompleted = 0;
    private int estimatedTotalChunks = 0;
    private final Map<String, Integer> finalWordCount = new ConcurrentHashMap<>();

    // =================== METRICS ===================
    private MetricsCollector metrics;
    private String runId;
    private Path runDir;

    // =================== ROUND ROBIN ===================
    private int roundRobinIndex = 0;
    private List<String> workerList;

    // =================== SETUP ===================

    @Override
    protected void setup() {
        parseArguments();
        initializeRun();
        initializeMetrics();

        // HDFS - ОБЯЗАТЕЛЬНАЯ ИНИЦИАЛИЗАЦИЯ
        if (!initializeHDFS()) {
            System.err.println("╔════════════════════════════════════════════════════╗");
            System.err.println("║   FATAL: HDFS INITIALIZATION FAILED               ║");
            System.err.println("╚════════════════════════════════════════════════════╝");
            System.err.println("Check HDFS URI: " + hdfsUri);
            System.err.println("Check file path: " + hdfsInputPath);
            doDelete();
            return;
        }

        if (currentMode == SchedulingMode.RL) {
            initializeRL();
        }

        registerMessageHandler();
        startHeartbeatMonitor();
        printBanner();
    }

    /**
     * Парсинг аргументов агента.
     *
     * Формат: MasterAgent(MODE, WORKERS, HDFS_URI, HDFS_PATH)
     * Пример: MasterAgent(RL, 4, hdfs://10.0.0.1:9000, /data/big6.txt)
     */
    private void parseArguments() {
        Object[] args = getArguments();

        if (args != null && args.length >= 1) {
            try {
                currentMode = SchedulingMode.valueOf(((String) args[0]).toUpperCase());
            } catch (IllegalArgumentException e) {
                System.out.println("Unknown mode, defaulting to RL");
            }
        }

        if (args != null && args.length >= 2) {
            expectedWorkers = Integer.parseInt((String) args[1]);
        }

        if (args != null && args.length >= 3) {
            hdfsUri = (String) args[2];
        }

        if (args != null && args.length >= 4) {
            hdfsInputPath = (String) args[3];
        }

        if (args != null && args.length >= 5) {
            maxChunkLines = Integer.parseInt((String) args[4]);
        }
    }

    /**
     * ИНИЦИАЛИЗАЦИЯ HDFS - ОБЯЗАТЕЛЬНЫЙ ЭТАП.
     *
     * @return true если HDFS подключен успешно
     */
    private boolean initializeHDFS() {
        System.out.println("\n╔════════════════════════════════════════════════════╗");
        System.out.println("║   INITIALIZING HDFS CONNECTION                     ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.println("HDFS URI:  " + hdfsUri);
        System.out.println("File path: " + hdfsInputPath);

        try {
            // Подключение к HDFS
            hdfsDataSource = new HDFSDataSource(hdfsUri);

            // Проверка существования файла
            if (!hdfsDataSource.exists(hdfsInputPath)) {
                System.err.println("ERROR: File not found in HDFS: " + hdfsInputPath);
                System.err.println("Upload file: hdfs dfs -put your_file.txt " + hdfsInputPath);
                return false;
            }

            // Получение информации о файле
            long fileSize = hdfsDataSource.getFileSize(hdfsInputPath);
            System.out.println("File size: " + (fileSize / 1024 / 1024) + " MB");

            // Получение информации о блоках (для data locality)
            blockLocations = hdfsDataSource.getBlockLocations(hdfsInputPath);
            System.out.println("HDFS blocks: " + blockLocations.size());

            for (BlockInfo block : blockLocations) {
                System.out.println("  " + block);
            }

            // Оценка количества chunk'ов
            long avgLineLength = 80;
            long estimatedLines = fileSize / avgLineLength;
            estimatedTotalChunks = (int) (estimatedLines / maxChunkLines) + 1;
            System.out.println("Estimated chunks: ~" + estimatedTotalChunks);

            // Создание итератора для чтения chunk'ов
            chunkIterator = hdfsDataSource.getChunkIterator(hdfsInputPath, maxChunkLines);

            System.out.println("✓ HDFS initialized successfully\n");
            return true;

        } catch (Exception e) {
            System.err.println("HDFS initialization error: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

    private void initializeRun() {
        runId = "run_" + currentMode.name().toLowerCase() + "_" +
                LocalDateTime.now().toString()
                        .replace(":", "-")
                        .replace(".", "-")
                        .substring(0, 19);

        runDir = Paths.get(System.getProperty("user.home"), "results", runId);

        try {
            Files.createDirectories(runDir.resolve("logs"));
            Files.createDirectories(runDir.resolve("metrics"));
        } catch (IOException e) {
            System.err.println("Failed to create directories: " + e.getMessage());
            doDelete();
        }
    }

    private void initializeMetrics() {
        metrics = new MetricsCollector(runDir, currentMode.name());
    }

    private void initializeRL() {
        rlPolicy = new QLearningPolicy(0.15, 0.85, 0.3, 0.02, 0.997);
        rewardCalculator = new RewardCalculator();

        try {
            Files.createDirectories(Paths.get(QTABLE_PATH).getParent());
            rlPolicy.loadFromFile(QTABLE_PATH);
        } catch (IOException e) {
            System.out.println("Starting with fresh Q-table");
        }
    }

    private void printBanner() {
        System.out.println("╔════════════════════════════════════════════════════╗");
        System.out.println("║   MAPREDUCE MASTER WITH HDFS                       ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.println("Mode:            " + currentMode);
        System.out.println("Expected workers: " + expectedWorkers);
        System.out.println("Chunk size:       " + maxChunkLines + " lines");
        System.out.println("HDFS URI:         " + hdfsUri);
        System.out.println("HDFS Path:        " + hdfsInputPath);
        System.out.println("Results dir:      " + runDir);

        if (currentMode == SchedulingMode.RL) {
            System.out.println("\nRL Configuration:");
            System.out.println("  ✓ System-level state space");
            System.out.println("  ✓ Proper Q-learning with nextState");
            System.out.println("  ✓ UCB-based exploration");
        }

        System.out.println("\n⏳ Waiting for " + expectedWorkers + " workers to register...\n");
    }

    // =================== MESSAGE HANDLING ===================

    private void registerMessageHandler() {
        addBehaviour(new CyclicBehaviour() {
            @Override
            public void action() {
                ACLMessage msg = receive();
                if (msg == null) {
                    block();
                    return;
                }

                String sender = msg.getSender().getLocalName();
                String content = msg.getContent();

                if (content != null) {
                    if (content.startsWith("HELLO")) {
                        handleWorkerRegistration(sender, content);
                    } else if (content.startsWith("HEARTBEAT")) {
                        handleHeartbeat(sender);
                    } else if (content.startsWith("CHUNK_TIME=")) {
                        handleWorkerResult(sender, content);
                    } else if (content.startsWith("WORKER_FAILED")) {
                        handleWorkerFailureDetected(sender);
                    }
                }

                if (msg.getPerformative() == ACLMessage.FAILURE) {
                    handleWorkerFailureDetected(sender);
                }
            }
        });
    }

    // =================== HEARTBEAT MONITORING ===================

    private void startHeartbeatMonitor() {
        addBehaviour(new TickerBehaviour(this, HEARTBEAT_CHECK_INTERVAL_MS) {
            @Override
            protected void onTick() {
                if (!mapReduceStarted) return;

                long now = System.currentTimeMillis();
                for (String worker : new HashSet<>(aliveWorkers)) {
                    Long lastHB = workerLastHeartbeat.get(worker);
                    if (lastHB != null && (now - lastHB) > HEARTBEAT_TIMEOUT_MS) {
                        System.out.println("⚠ HEARTBEAT TIMEOUT: " + worker);
                        handleWorkerFailureDetected(worker);
                    }
                }
            }
        });
    }

    private void handleHeartbeat(String worker) {
        workerLastHeartbeat.put(worker, System.currentTimeMillis());
        if (failedWorkers.contains(worker)) {
            handleWorkerRecovery(worker);
        }
    }

    // =================== WORKER REGISTRATION ===================

    private void handleWorkerRegistration(String workerName, String content) {
        if (aliveWorkers.contains(workerName)) return;

        int cores = Runtime.getRuntime().availableProcessors();
        long maxMem = Runtime.getRuntime().maxMemory();
        String hostname = "unknown";

        if (content.contains("|")) {
            for (String part : content.split("\\|")) {
                if (part.startsWith("cores=")) {
                    cores = Integer.parseInt(part.substring(6));
                } else if (part.startsWith("maxMemMB=")) {
                    maxMem = Long.parseLong(part.substring(9)) * 1024 * 1024;
                } else if (part.startsWith("hostname=")) {
                    hostname = part.substring(9);
                }
            }
        }

        aliveWorkers.add(workerName);
        workerBusy.put(workerName, false);
        workerChunkCount.put(workerName, 0);
        workerTotalTime.put(workerName, 0L);
        workerAvgTime.put(workerName, 0.0);
        workerLastHeartbeat.put(workerName, System.currentTimeMillis());
        workerCores.put(workerName, cores);
        workerMaxMemory.put(workerName, maxMem);
        workerHostname.put(workerName, hostname);
        workerPendingChunks.put(workerName, new ArrayList<>());

        metrics.registerWorker(workerName);

        System.out.println("✓ Worker: " + workerName +
                " (cores=" + cores + ", mem=" + (maxMem / 1024 / 1024) + "MB, host=" + hostname + ")" +
                " [" + aliveWorkers.size() + "/" + expectedWorkers + "]");

        // Check data locality
        checkDataLocality(workerName, hostname);

        if (!mapReduceStarted && aliveWorkers.size() == expectedWorkers) {
            workerList = new ArrayList<>(aliveWorkers);
            Collections.sort(workerList);
            startMapReduce();
        }
    }

    /**
     * Проверяет, есть ли блоки HDFS на узле worker'а (data locality).
     */
    private void checkDataLocality(String workerName, String hostname) {
        if (blockLocations == null || hostname.equals("unknown")) return;

        int localBlocks = 0;
        for (BlockInfo block : blockLocations) {
            for (String host : block.hosts) {
                if (host.contains(hostname) || hostname.contains(host)) {
                    localBlocks++;
                    break;
                }
            }
        }

        if (localBlocks > 0) {
            System.out.println("  → Data locality: " + localBlocks + " blocks on " + hostname);
        }
    }

    // =================== FAULT TOLERANCE ===================

    private void handleWorkerFailureDetected(String worker) {
        if (failedWorkers.contains(worker)) return;

        long detectionTime = System.currentTimeMillis() -
                workerLastHeartbeat.getOrDefault(worker, System.currentTimeMillis());

        System.out.println("\n⚠ WORKER FAILURE: " + worker + " (detection: " + detectionTime + "ms)");

        failedWorkers.add(worker);
        aliveWorkers.remove(worker);
        workerBusy.put(worker, false);

        metrics.recordWorkerFailure(worker, "heartbeat_timeout");
        metrics.recordFailureDetected(worker, detectionTime);

        // Redistribute pending chunks
        List<PendingChunk> pending = workerPendingChunks.getOrDefault(worker, new ArrayList<>());
        for (PendingChunk chunk : pending) {
            redistributeChunk(chunk, worker);
        }
        workerPendingChunks.put(worker, new ArrayList<>());

        workerList = new ArrayList<>(aliveWorkers);
        Collections.sort(workerList);
    }

    private void redistributeChunk(PendingChunk chunk, String failedWorker) {
        String newWorker = aliveWorkers.stream()
                .filter(w -> !workerBusy.get(w))
                .min(Comparator.comparing(w -> workerChunkCount.get(w)))
                .orElse(aliveWorkers.stream().findFirst().orElse(null));

        if (newWorker == null) {
            System.err.println("No workers for redistribution!");
            return;
        }

        ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
        msg.addReceiver(new AID(newWorker, AID.ISLOCALNAME));
        msg.setContent(chunk.content);
        send(msg);

        workerBusy.put(newWorker, true);
        workerPendingChunks.get(newWorker).add(chunk);

        System.out.println("→ Chunk #" + chunk.chunkId + " redistributed: " +
                failedWorker + " → " + newWorker);
        metrics.recordChunkRedistribution(failedWorker, newWorker, chunk.chunkId, 0);
    }

    private void handleWorkerRecovery(String worker) {
        System.out.println("✓ Worker recovered: " + worker);
        failedWorkers.remove(worker);
        aliveWorkers.add(worker);
        workerList = new ArrayList<>(aliveWorkers);
        Collections.sort(workerList);
        metrics.recordWorkerRecovery(worker, 0);
    }

    // =================== MAPREDUCE EXECUTION ===================

    private void startMapReduce() {
        globalStartTime = System.currentTimeMillis();
        mapReduceStarted = true;

        System.out.println("\n╔════════════════════════════════════════════════════╗");
        System.out.println("║   MAPREDUCE STARTED (READING FROM HDFS)            ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.println("Source: " + hdfsUri + hdfsInputPath);
        System.out.println();

        // Dispatch initial chunks to all workers
        for (int i = 0; i < aliveWorkers.size(); i++) {
            dispatchNextChunk();
        }
    }

    // =================== CHUNK DISPATCHING (FROM HDFS) ===================

    /**
     * Читает следующий chunk из HDFS и отправляет worker'у.
     */
    private void dispatchNextChunk() {
        if (eofReached) return;

        String selectedWorker = selectWorker();
        if (selectedWorker == null) {
            // Нет свободных воркеров — НЕ EOF
            return;
        }

        try {
            if (!chunkIterator.hasNext()) {
                eofReached = true;
                System.out.println("\n📁 HDFS: End of file reached\n");
                return;
            }

            ChunkData chunkData = chunkIterator.next();
            if (chunkData == null) {
                eofReached = true;
                return;
            }

            totalChunksDispatched++;
            int chunkId = totalChunksDispatched;

            ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
            msg.addReceiver(new AID(selectedWorker, AID.ISLOCALNAME));
            msg.setContent(chunkData.content);
            send(msg);

            workerBusy.put(selectedWorker, true);

            PendingChunk pending = new PendingChunk();
            pending.chunkId = chunkId;
            pending.content = chunkData.content;
            pending.lines = chunkData.lines;
            workerPendingChunks.get(selectedWorker).add(pending);

            int imbalance = rewardCalculator.calculateImbalance(workerChunkCount);
            metrics.recordChunkDispatched(
                    chunkId,
                    selectedWorker,
                    chunkData.lines,
                    countBusyWorkers(),
                    imbalance
            );

            System.out.println("→ Chunk #" + chunkId + " → " + selectedWorker +
                    " (" + chunkData.lines + " lines)");

        } catch (Exception e) {
            // ⚠️ НЕ EOF
            System.err.println("Dispatch error (NOT EOF):");
            e.printStackTrace();
        }
    }


    // =================== WORKER SELECTION ===================

    private String selectWorker() {
        switch (currentMode) {
            case RL:
                return selectWorkerRL();
            case ROUND_ROBIN:
                return selectWorkerRoundRobin();
            case LEAST_LOADED:
                return selectWorkerLeastLoaded();
            default:
                return selectWorkerLeastLoaded();
        }
    }

    private String selectWorkerRL() {
        Set<String> freeWorkers = aliveWorkers.stream()
                .filter(w -> !workerBusy.get(w))
                .collect(Collectors.toSet());

        if (freeWorkers.isEmpty()) return null;

        Set<String> busySet = aliveWorkers.stream()
                .filter(w -> workerBusy.get(w))
                .collect(Collectors.toSet());

        RLState currentState = new RLState(
                workerChunkCount, workerAvgTime, busySet,
                aliveWorkers.size(), totalChunksCompleted, estimatedTotalChunks);

        String selectedWorker = rlPolicy.selectAction(currentState, freeWorkers);

        lastState = currentState;
        lastAction = selectedWorker;
        lastChunkId = totalChunksDispatched + 1;
        lastQValue = rlPolicy.getQValue(currentState, selectedWorker);

        return selectedWorker;
    }

    private String selectWorkerRoundRobin() {
        if (workerList == null || workerList.isEmpty()) return null;

        int attempts = 0;
        while (attempts < workerList.size()) {
            String worker = workerList.get(roundRobinIndex);
            roundRobinIndex = (roundRobinIndex + 1) % workerList.size();

            if (!workerBusy.get(worker) && !failedWorkers.contains(worker)) {
                return worker;
            }
            attempts++;
        }
        return null;
    }

    private String selectWorkerLeastLoaded() {
        return aliveWorkers.stream()
                .filter(w -> !workerBusy.get(w))
                .min(Comparator.comparing(w -> workerChunkCount.get(w)))
                .orElse(null);
    }


// =================== RESULT HANDLING ===================

    private void handleWorkerResult(String worker, String content) {
        // CRITICAL FIX: Null-safety checks
        if (worker == null || content == null) {
            System.err.println("ERROR: Null worker or content in handleWorkerResult");
            return;
        }

        if (!aliveWorkers.contains(worker)) {
            System.err.println("WARNING: Result from unknown/dead worker: " + worker);
            return;
        }

        WorkerResult result = parseWorkerResult(content);

        // FIX: Safe handling of pending chunks
        List<PendingChunk> pending = workerPendingChunks.get(worker);
        if (pending != null && !pending.isEmpty()) {
            pending.remove(0);
        } else {
            System.err.println("WARNING: No pending chunks for worker " + worker);
        }

        // RL Update - with null checks
        if (currentMode == SchedulingMode.RL && lastState != null && lastAction != null) {
            try {
                updateRLPolicy(result, worker);
            } catch (Exception e) {
                System.err.println("ERROR in RL update: " + e.getMessage());
                e.printStackTrace();
            }
        }

        // Merge word counts - with null check
        if (result.wordCount != null) {
            for (Map.Entry<String, Integer> entry : result.wordCount.entrySet()) {
                finalWordCount.merge(entry.getKey(), entry.getValue(), Integer::sum);
            }
        }

        // Update statistics - with null safety
        Integer currentChunks = workerChunkCount.get(worker);
        if (currentChunks == null) {
            System.err.println("WARNING: Worker " + worker + " not in workerChunkCount map, initializing");
            currentChunks = 0;
        }
        int completedChunks = currentChunks + 1;
        workerChunkCount.put(worker, completedChunks);

        Long currentTotalTime = workerTotalTime.get(worker);
        if (currentTotalTime == null) {
            System.err.println("WARNING: Worker " + worker + " not in workerTotalTime map, initializing");
            currentTotalTime = 0L;
        }
        long totalTime = currentTotalTime + result.processingTime;
        workerTotalTime.put(worker, totalTime);
        workerAvgTime.put(worker, (double) totalTime / completedChunks);

        workerLastFinishTime.put(worker, System.currentTimeMillis());
        workerLastHeartbeat.put(worker, System.currentTimeMillis());

        totalChunksCompleted++;
        totalLinesProcessed += result.lines;

        // Metrics - with null check
        int imbalance = rewardCalculator != null ?
                rewardCalculator.calculateImbalance(workerChunkCount) : 0;

        if (metrics != null) {
            try {
                metrics.recordChunkCompleted(totalChunksCompleted, worker, result.lines,
                        result.processingTime, result.memBefore, result.memAfter, imbalance);
            } catch (Exception e) {
                System.err.println("ERROR recording metrics: " + e.getMessage());
            }
        }

        // Progress reporting
        if (totalChunksCompleted % 10 == 0) {
            if (metrics != null) {
                try {
                    metrics.recordSystemSnapshot(totalChunksCompleted, totalChunksDispatched,
                            countBusyWorkers(), aliveWorkers.size(), totalLinesProcessed);
                } catch (Exception e) {
                    System.err.println("ERROR recording system snapshot: " + e.getMessage());
                }
            }
            System.out.println("📊 Progress: " + totalChunksCompleted + " chunks | " +
                    totalLinesProcessed + " lines (from HDFS)");
        }

        // Mark worker as free
        workerBusy.put(worker, false);

// KEEP PIPELINE FULL
        while (!eofReached) {
            String nextWorker = selectWorker();
            if (nextWorker == null) {
                break;
            }
            dispatchNextChunk();
        }


        // Check for completion
        // Check for completion (CORRECT CONDITION)
        if (eofReached && totalChunksCompleted == totalChunksDispatched) {
            try {
                finishMapReduce();
            } catch (Exception e) {
                System.err.println("ERROR finishing MapReduce: " + e.getMessage());
                e.printStackTrace();
            }
        }

    }

    private void updateRLPolicy(WorkerResult result, String worker) {
        Set<String> busySet = aliveWorkers.stream()
                .filter(w -> workerBusy.get(w))
                .collect(Collectors.toSet());

        RLState nextState = new RLState(
                workerChunkCount, workerAvgTime, busySet,
                aliveWorkers.size(), totalChunksCompleted, estimatedTotalChunks);

        Map<String, RewardCalculator.WorkerStats> workerStats = new HashMap<>();
        for (String w : aliveWorkers) {
            workerStats.put(w, new RewardCalculator.WorkerStats(
                    workerAvgTime.get(w), workerChunkCount.get(w)));
        }

        int imbalanceAfter = rewardCalculator.calculateImbalance(workerChunkCount);
        double reward = rewardCalculator.compute(
                result.processingTime, worker, workerStats, imbalanceAfter);

        rlPolicy.update(lastState, lastAction, reward, nextState, aliveWorkers);
        double newQ = rlPolicy.getQValue(lastState, lastAction);

        if (currentMode == SchedulingMode.RL) {
            metrics.recordRLStep(
                    lastChunkId,
                    lastState.toString(),
                    lastAction,
                    reward,
                    rlPolicy.getEpsilon(),
                    lastQValue,
                    newQ,
                    false
            );

            lastState = null;
            lastAction = null;
        }

    }

    // =================== COMPLETION ===================

    private void finishMapReduce() {
        long totalTime = System.currentTimeMillis() - globalStartTime;

        // Close HDFS resources
        try {
            if (chunkIterator != null) {
                chunkIterator.close();
            }
            if (hdfsDataSource != null) {
                hdfsDataSource.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing HDFS: " + e.getMessage());
        }

        System.out.println("\n╔════════════════════════════════════════════════════╗");
        System.out.println("║   MAPREDUCE COMPLETED (HDFS SOURCE)               ║");
        System.out.println("╚════════════════════════════════════════════════════╝");
        System.out.println("Total time:  " + totalTime + " ms (" +
                String.format("%.2f", totalTime / 1000.0) + " s)");
        System.out.println("Total chunks: " + totalChunksCompleted);
        System.out.println("Total lines:  " + totalLinesProcessed);
        System.out.println("Throughput:   " +
                String.format("%.2f", totalLinesProcessed * 1000.0 / totalTime) + " lines/s");
        System.out.println("HDFS source:  " + hdfsUri + hdfsInputPath);

        writeResults(totalTime);
        metrics.writeFinalSummary(totalTime, totalChunksCompleted, totalLinesProcessed);

        if (currentMode == SchedulingMode.RL) {
            rlPolicy.saveToFile(QTABLE_PATH);
            rlPolicy.exportLearningHistory(runDir.resolve("metrics/rl_history.csv").toString());
            rlPolicy.printStatistics();
            rlPolicy.printTopQValues(15);
        }

        printWorkerPerformance();
        System.out.println("\n✓ Results saved to: " + runDir);
        doDelete();
    }

    // =================== UTILITIES ===================

    private int countBusyWorkers() {
        return (int) aliveWorkers.stream().filter(w -> workerBusy.get(w)).count();
    }

    private WorkerResult parseWorkerResult(String message) {
        WorkerResult result = new WorkerResult();
        result.wordCount = new HashMap<>();

        for (String line : message.split("\n")) {
            if (line.startsWith("CHUNK_TIME=")) {
                result.processingTime = Long.parseLong(line.substring(11));
            } else if (line.startsWith("LINES=")) {
                result.lines = Integer.parseInt(line.substring(6));
            } else if (line.startsWith("MEM_BEFORE=")) {
                result.memBefore = Long.parseLong(line.substring(11));
            } else if (line.startsWith("MEM_AFTER=")) {
                result.memAfter = Long.parseLong(line.substring(10));
            } else if (line.startsWith("DATA=")) {
                String data = line.substring(5).replace("{", "").replace("}", "").trim();
                if (!data.isEmpty()) {
                    for (String entry : data.split(",")) {
                        String[] kv = entry.trim().split("=");
                        if (kv.length == 2) {
                            result.wordCount.put(kv[0].trim(), Integer.parseInt(kv[1].trim()));
                        }
                    }
                }
            }
        }
        return result;
    }

    private void writeResults(long totalTime) {
        try {
            // Word count
            try (BufferedWriter w = Files.newBufferedWriter(runDir.resolve("wordcount.txt"))) {
                finalWordCount.entrySet().stream()
                        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                        .limit(1000)
                        .forEach(e -> {
                            try {
                                w.write(e.getKey() + "=" + e.getValue() + "\n");
                            } catch (IOException ex) {
                            }
                        });
            }

            // Summary
            try (BufferedWriter w = Files.newBufferedWriter(runDir.resolve("summary.txt"))) {
                w.write("=== MapReduce Execution Summary ===\n\n");
                w.write("Run ID: " + runId + "\n");
                w.write("Mode: " + currentMode + "\n");
                w.write("HDFS Source: " + hdfsUri + hdfsInputPath + "\n\n");
                w.write("Time: " + totalTime + " ms\n");
                w.write("Chunks: " + totalChunksCompleted + "\n");
                w.write("Lines: " + totalLinesProcessed + "\n");
                w.write("Throughput: " + String.format("%.2f", totalLinesProcessed * 1000.0 / totalTime) + " lines/s\n");
                w.write("Failed workers: " + failedWorkers.size() + "\n");
            }

        } catch (IOException e) {
            System.err.println("Error writing results: " + e.getMessage());
        }
    }

    private void printWorkerPerformance() {
        System.out.println("\n═══ WORKER PERFORMANCE ═══");

        Set<String> all = new HashSet<>(aliveWorkers);
        all.addAll(failedWorkers);

        List<String> sortedWorkers = all.stream().sorted().collect(Collectors.toList());
        for (String w : sortedWorkers) {
            int chunks = workerChunkCount.getOrDefault(w, 0);
            double avg = workerAvgTime.getOrDefault(w, 0.0);
            double pct = totalChunksCompleted > 0 ? chunks * 100.0 / totalChunksCompleted : 0;
            String status = failedWorkers.contains(w) ? " [FAILED]" : "";

            System.out.printf("  %s: %d chunks (%.1f%%), avg %.0fms%s%n",
                    w, chunks, pct, avg, status);
        }
    }

    // =================== INNER CLASSES ===================

    private static class WorkerResult {
        long processingTime;
        int lines;
        long memBefore;
        long memAfter;
        Map<String, Integer> wordCount;
    }

    private static class PendingChunk {
        int chunkId;
        String content;
        int lines;
    }
}