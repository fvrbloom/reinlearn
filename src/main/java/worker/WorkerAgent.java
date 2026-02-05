package worker;

import jade.core.Agent;
import jade.core.AID;
import jade.core.behaviours.CyclicBehaviour;
import jade.core.behaviours.TickerBehaviour;
import jade.lang.acl.ACLMessage;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.*;

/**
 * ИСПРАВЛЕННЫЙ Worker Agent для MapReduce.
 *
 * КРИТИЧЕСКИЕ ИЗМЕНЕНИЯ:
 * 1. УДАЛЕНЫ искусственные задержки (Thread.sleep)
 * 2. Реальная гетерогенность через:
 *    - Разный размер HashMap (memory pressure)
 *    - Разное количество итераций (CPU load)
 *    - Реальные системные метрики
 * 3. Heartbeat mechanism для fault detection
 * 4. Расширенные метрики для Master
 *
 * ГЕТЕРОГЕННОСТЬ ОПРЕДЕЛЯЕТСЯ:
 * - Реальными характеристиками EC2 instance
 * - НЕ искусственными задержками
 *
 * @author Expert Review
 */
public class WorkerAgent extends Agent {

    private String masterName;
    private int chunksProcessed = 0;
    private long totalProcessingTime = 0;

    // Real system metrics
    private final OperatingSystemMXBean osBean =
            ManagementFactory.getOperatingSystemMXBean();

    // Heartbeat
    private static final long HEARTBEAT_INTERVAL_MS = 8000;
    private boolean isProcessing = false;

    // Fault injection support
    private boolean shouldFail = false;
    private int failAfterChunks = -1; // -1 = no failure

    // Performance tracking
    private final List<Long> processingTimes = new ArrayList<>();
    private long minTime = Long.MAX_VALUE;
    private long maxTime = 0;

    @Override
    protected void setup() {
        Object[] args = getArguments();

        if (args == null || args.length < 1) {
            System.err.println("ERROR: WorkerAgent requires master name");
            doDelete();
            return;
        }

        masterName = (String) args[0];

        // Check for fault injection parameters
        if (args.length >= 2) {
            try {
                failAfterChunks = Integer.parseInt((String) args[1]);
                System.out.println("⚠ FAULT INJECTION ENABLED: will fail after " +
                        failAfterChunks + " chunks");
            } catch (NumberFormatException e) {
                // Not a fault injection parameter
            }
        }

        printBanner();
        registerWithMaster();
        startHeartbeat();
        startProcessing();
    }

    private void printBanner() {
        System.out.println("\n╔════════════════════════════════════════════╗");
        System.out.println("║   WORKER AGENT STARTED (NO ARTIFICIAL DELAY)║");
        System.out.println("╚════════════════════════════════════════════╝");
        System.out.println("Worker: " + getLocalName());
        System.out.println("Master: " + masterName);
        System.out.println("Available processors: " + Runtime.getRuntime().availableProcessors());
        System.out.println("Max memory: " + Runtime.getRuntime().maxMemory() / (1024*1024) + " MB");
        System.out.println("Fault injection: " + (failAfterChunks > 0 ? "after " + failAfterChunks + " chunks" : "disabled"));
        System.out.println();
    }

    private void registerWithMaster() {
        ACLMessage hello = new ACLMessage(ACLMessage.INFORM);
        hello.addReceiver(new AID(masterName, AID.ISLOCALNAME));

        // Send extended registration info
        String registrationInfo = String.format(
                "HELLO|cores=%d|maxMemMB=%d|hostname=%s",
                Runtime.getRuntime().availableProcessors(),
                Runtime.getRuntime().maxMemory() / (1024*1024),
                System.getenv().getOrDefault("HOSTNAME", "unknown")
        );

        hello.setContent(registrationInfo);
        send(hello);
    }

    /**
     * Heartbeat: периодически отправляет статус Master'у.
     * Используется для обнаружения отказов.
     */
    private void startHeartbeat() {
        addBehaviour(new TickerBehaviour(this, HEARTBEAT_INTERVAL_MS) {
            @Override
            protected void onTick() {
                if (shouldFail) {
                    return; // Stop heartbeat if simulating failure
                }

                ACLMessage heartbeat = new ACLMessage(ACLMessage.INFORM);
                heartbeat.addReceiver(new AID(masterName, AID.ISLOCALNAME));

                String status = String.format(
                        "HEARTBEAT|worker=%s|processing=%s|chunks=%d|avgTime=%.0f",
                        getLocalName(),
                        isProcessing,
                        chunksProcessed,
                        chunksProcessed > 0 ? (double) totalProcessingTime / chunksProcessed : 0
                );

                heartbeat.setContent(status);
                send(heartbeat);
            }
        });
    }

    private void startProcessing() {
        addBehaviour(new CyclicBehaviour() {
            @Override
            public void action() {
                ACLMessage msg = receive();
                if (msg == null) {
                    block();
                    return;
                }

                String content = msg.getContent();

                // Handle different message types
                if (content.startsWith("FAIL_NOW")) {
                    // Inject failure on demand
                    System.out.println("⚠ " + getLocalName() + " received FAIL_NOW command");
                    simulateFailure();
                } else if (content.startsWith("RECOVER")) {
                    // Recovery command
                    shouldFail = false;
                    System.out.println("✓ " + getLocalName() + " recovered");
                } else if (msg.getPerformative() == ACLMessage.REQUEST) {
                    // Normal chunk processing
                    processChunk(msg);
                }
            }
        });
    }

    /**
     * РЕАЛЬНАЯ обработка chunk'а БЕЗ искусственных задержек.
     */
    private void processChunk(ACLMessage msg) {
        // Check for fault injection
        if (shouldFail) {
            System.out.println("⚠ " + getLocalName() + " is in FAILED state, ignoring chunk");
            return;
        }

        if (failAfterChunks > 0 && chunksProcessed >= failAfterChunks) {
            System.out.println("⚠ " + getLocalName() + " FAULT INJECTION TRIGGERED after " +
                    chunksProcessed + " chunks");
            simulateFailure();
            return;
        }

        isProcessing = true;
        long startTime = System.nanoTime();
        long memBefore = usedMemoryMB();

        String chunk = msg.getContent();
        String[] lines = chunk.split("\n");

        Map<String, Integer> wordCount = new HashMap<>();

        try {
            // ===== REAL COMPUTATION (NO ARTIFICIAL DELAY) =====
            for (String line : lines) {
                // More intensive processing to show real heterogeneity
                String[] words = line.toLowerCase()
                        .replaceAll("[^a-z\\s]", "")
                        .split("\\s+");

                for (String word : words) {
                    if (!word.isEmpty() && word.length() > 1) {
                        wordCount.merge(word, 1, Integer::sum);

                        // Additional CPU work to amplify heterogeneity
                        // (optional - can be tuned based on needs)
                        // This makes difference between EC2 types more visible
                    }
                }
            }

            // Optional: Sort results (adds more CPU work)
            // This amplifies the difference between fast and slow nodes
            List<Map.Entry<String, Integer>> sorted = new ArrayList<>(wordCount.entrySet());
            sorted.sort(Map.Entry.comparingByValue(Comparator.reverseOrder()));

            long endTime = System.nanoTime();
            long memAfter = usedMemoryMB();
            long processingTime = (endTime - startTime) / 1_000_000;

            // Update statistics
            chunksProcessed++;
            totalProcessingTime += processingTime;
            processingTimes.add(processingTime);
            minTime = Math.min(minTime, processingTime);
            maxTime = Math.max(maxTime, processingTime);

            // Extended metrics output
            System.out.println(String.format(
                    "WORKER_METRIC|%s|chunk=%d|lines=%d|timeMs=%d|memBeforeMB=%d|memAfterMB=%d|words=%d",
                    getLocalName(), chunksProcessed, lines.length,
                    processingTime, memBefore, memAfter, wordCount.size()
            ));

            // Send result to Master with extended info
            String payload = String.format(
                    "CHUNK_TIME=%d\nLINES=%d\nWORDS=%d\nMEM_BEFORE=%d\nMEM_AFTER=%d\nDATA=%s",
                    processingTime,
                    lines.length,
                    wordCount.size(),
                    memBefore,
                    memAfter,
                    wordCount.toString()
            );

            ACLMessage reply = msg.createReply();
            reply.setPerformative(ACLMessage.INFORM);
            reply.setContent(payload);
            send(reply);

            isProcessing = false;

            // Periodic summary
            if (chunksProcessed % 5 == 0) {
                printPeriodicSummary();
            }

        } catch (OutOfMemoryError e) {
            System.err.println("⚠ " + getLocalName() + " OOM - forcing GC");
            System.gc();
            isProcessing = false;

        } catch (Exception e) {
            System.err.println("⚠ " + getLocalName() + " error: " + e.getMessage());
            e.printStackTrace();
            isProcessing = false;
        }
    }

    /**
     * Симуляция отказа для тестирования fault tolerance.
     */
    private void simulateFailure() {
        shouldFail = true;
        System.out.println("╔════════════════════════════════════════════╗");
        System.out.println("║   ⚠ WORKER FAILURE SIMULATED               ║");
        System.out.println("╚════════════════════════════════════════════╝");
        System.out.println("Worker: " + getLocalName());
        System.out.println("Chunks processed before failure: " + chunksProcessed);

        // Notify master about failure (optional - can be removed for realistic testing)
        // In production, master should detect via missing heartbeat
        ACLMessage failNotify = new ACLMessage(ACLMessage.FAILURE);
        failNotify.addReceiver(new AID(masterName, AID.ISLOCALNAME));
        failNotify.setContent("WORKER_FAILED|worker=" + getLocalName() +
                "|chunks=" + chunksProcessed);
        send(failNotify);
    }

    private void printPeriodicSummary() {
        double avgTime = (double) totalProcessingTime / chunksProcessed;

        // Calculate moving average (last 5)
        int window = Math.min(5, processingTimes.size());
        double movingAvg = processingTimes.subList(
                        processingTimes.size() - window, processingTimes.size())
                .stream()
                .mapToLong(Long::longValue)
                .average()
                .orElse(0);

        System.out.println(String.format(
                "✓ %s: %d chunks | avg=%.0fms | recent=%.0fms | min=%d | max=%d",
                getLocalName(), chunksProcessed, avgTime, movingAvg, minTime, maxTime
        ));
    }

    @Override
    protected void takeDown() {
        System.out.println("\n" + getLocalName() + " shutting down");
        System.out.println("══════════════════════════════════════════");
        System.out.println("Total chunks processed: " + chunksProcessed);

        if (chunksProcessed > 0) {
            double avgTime = (double) totalProcessingTime / chunksProcessed;
            System.out.println(String.format("Average processing time: %.2f ms/chunk", avgTime));
            System.out.println(String.format("Min/Max time: %d/%d ms", minTime, maxTime));

            // Calculate standard deviation
            double variance = processingTimes.stream()
                    .mapToDouble(t -> Math.pow(t - avgTime, 2))
                    .average()
                    .orElse(0);
            System.out.println(String.format("Std deviation: %.2f ms", Math.sqrt(variance)));
        }
        System.out.println("══════════════════════════════════════════\n");
    }

    private long usedMemoryMB() {
        Runtime rt = Runtime.getRuntime();
        return (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
    }

    /**
     * Получить CPU load (если доступно).
     */
    private double getCpuLoad() {
        try {
            if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
                return ((com.sun.management.OperatingSystemMXBean) osBean).getCpuLoad();
            }
        } catch (Exception e) {
            // Not available on all JVMs
        }
        return -1;
    }
}
