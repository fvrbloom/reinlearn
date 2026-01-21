package coordinator;

import jade.core.Agent;
import jade.core.AID;
import jade.core.behaviours.CyclicBehaviour;
import jade.lang.acl.ACLMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.util.*;

public class MasterAgent extends Agent {

    // ================= CONFIG =================
    private static final int EXPECTED_WORKERS = 4;
    private static final int MAX_CHUNK_LINES = 20_000;
    private static final String INPUT_FILE = "big2.txt";

    // ================= RUN CONTEXT =================
    private String runId;
    private Path runDir;

    // ================= STATE ==================
    private int activeWorkers = 0;

    private final Set<String> aliveWorkers = new HashSet<>();
    private final Map<String, Boolean> workerBusy = new HashMap<>();
    private final Map<String, Integer> workerChunkCounter = new HashMap<>();

    private final Map<String, Long> workerTotalTime = new HashMap<>();
    private final Map<String, Long> workerFinishTime = new HashMap<>();

    private BufferedReader reader;
    private boolean eofReached = false;
    private boolean mapReduceStarted = false;

    private long globalStartTime;
    private long globalEndTime;
    private int totalLinesCount = 0;
    private int totalChunks = 0;

    private final Map<String, Integer> finalWordCount = new HashMap<>();

    // ================= SETUP ==================
    @Override
    protected void setup() {

        runId = LocalDateTime.now()
                .toString()
                .replace(":", "-")
                .replace(".", "-");

        runDir = Paths.get(
                System.getProperty("user.home"),
                "results",
                "run_" + runId
        );

        try {
            Files.createDirectories(runDir);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("MASTER started");
        System.out.println("MASTER run directory = " + runDir);

        logEvent("MASTER_STARTED", "master", null);
        System.out.println("MASTER: waiting for workers...");

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

                // ===== WORKER REGISTRATION =====
                if ("HELLO".equals(content)) {

                    if (aliveWorkers.add(sender)) {
                        workerBusy.put(sender, false);
                        workerChunkCounter.put(sender, 0);
                        workerTotalTime.put(sender, 0L);

                        logEvent("WORKER_REGISTERED", sender, null);
                        System.out.println("MASTER: worker registered -> " + sender);
                    }

                    if (!mapReduceStarted &&
                            aliveWorkers.size() == EXPECTED_WORKERS) {
                        startMapReduce();
                    }
                    return;
                }

                // ===== IGNORE NON-DATA =====
                if (content == null || !content.startsWith("CHUNK_TIME=")) {
                    return;
                }

                // ===== WORKER RESULT =====
                WorkerResult wr = parseWorkerResult(content);

                for (Map.Entry<String, Integer> e : wr.wordCount.entrySet()) {
                    finalWordCount.merge(e.getKey(), e.getValue(), Integer::sum);
                }

                int chunkId = workerChunkCounter.get(sender) + 1;
                workerChunkCounter.put(sender, chunkId);
                workerTotalTime.put(
                        sender,
                        workerTotalTime.get(sender) + wr.processingTime
                );

                workerFinishTime.put(sender, System.currentTimeMillis());
                totalChunks++;

                writeChunkLog(sender, chunkId, wr.lines, wr.processingTime);

                workerBusy.put(sender, false);
                activeWorkers--;              // ← ВАЖНО
                sendNextChunk(sender);


                if (eofReached && activeWorkers == 0) {
                    finishReduce();
                }

            }
        });
    }

    // ================= MAPREDUCE =================

    private void startMapReduce() {
        try {
            initChunkLog();

            logEvent(
                    "MAPREDUCE_STARTED",
                    "master",
                    "workers=" + aliveWorkers.size()
            );

            reader = Files.newBufferedReader(
                    Paths.get(
                            System.getProperty("user.home"),
                            "data",
                            INPUT_FILE
                    )
            );

            globalStartTime = System.currentTimeMillis();
            mapReduceStarted = true;

            for (String worker : aliveWorkers) {
                sendNextChunk(worker);
            }

        } catch (Exception e) {
            e.printStackTrace();
            doDelete();
        }
    }

    private void sendNextChunk(String worker) {
        if (workerBusy.get(worker) || eofReached) return;

        try {
            StringBuilder buffer = new StringBuilder();
            int lines = 0;
            String line;

            while (lines < MAX_CHUNK_LINES &&
                    (line = reader.readLine()) != null) {
                buffer.append(line).append("\n");
                lines++;
                totalLinesCount++;
            }

            if (buffer.length() == 0) {
                eofReached = true;
                return;
            }

            ACLMessage msg = new ACLMessage(ACLMessage.REQUEST);
            msg.addReceiver(new AID(worker, AID.ISLOCALNAME));
            msg.setContent(buffer.toString());

            send(msg);
            logEvent(
                    "CHUNK_DISPATCHED",
                    "master",
                    "worker=" + worker
            );

            System.out.println("Sent chunk to " + worker);

            workerBusy.put(worker, true);
            activeWorkers++;

        } catch (Exception e) {
            e.printStackTrace();
            doDelete();
        }
    }

    private boolean allWorkersIdle() {
        return workerBusy.values().stream().noneMatch(b -> b);
    }

    private void finishReduce() {
        System.out.println(
                "MASTER: finishReduce() called, activeWorkers=" + activeWorkers
        );

        globalEndTime = System.currentTimeMillis();
        long totalTime = globalEndTime - globalStartTime;

        writeFinalWordCount();
        writeRunSummary(totalTime);
        writeMetricsABCD(totalTime);

        logEvent(
                "MAPREDUCE_FINISHED",
                "master",
                "total_time_ms=" + totalTime
        );

        doDelete();
    }

    // ================= METRICS =================

    private void writeMetricsABCD(long totalTime) {
        try {
            Path file = runDir.resolve("metrics.csv");
            BufferedWriter w = Files.newBufferedWriter(file);

            w.write("metric,value");
            w.newLine();

            // A: Throughput
            w.write("lines_per_sec," + (totalLinesCount * 1000.0 / totalTime));
            w.newLine();
            w.write("chunks_per_sec," + (totalChunks * 1000.0 / totalTime));
            w.newLine();

            // B: Load balance
            Collection<Integer> chunks = workerChunkCounter.values();
            w.write("min_chunks," + Collections.min(chunks));
            w.newLine();
            w.write("max_chunks," + Collections.max(chunks));
            w.newLine();

            // C: Tail latency
            long makespan =
                    Collections.max(workerFinishTime.values()) - globalStartTime;
            w.write("makespan_ms," + makespan);
            w.newLine();

            w.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= PARSING =================

    private WorkerResult parseWorkerResult(String msg) {
        WorkerResult wr = new WorkerResult();
        wr.wordCount = new HashMap<>();

        String[] lines = msg.split("\n");

        wr.processingTime = Long.parseLong(lines[0].substring(11));
        wr.lines = Integer.parseInt(lines[1].substring(6));

        String data = lines[2]
                .substring(5)
                .replace("{", "")
                .replace("}", "")
                .trim();

        if (!data.isEmpty()) {
            for (String e : data.split(",")) {
                String[] kv = e.trim().split("=");
                if (kv.length == 2) {
                    wr.wordCount.put(
                            kv[0].trim(),
                            Integer.parseInt(kv[1].trim())
                    );
                }
            }
        }
        return wr;
    }

    private static class WorkerResult {
        long processingTime;
        int lines;
        Map<String, Integer> wordCount;
    }

    // ================= FILE OUTPUT =================

    private void initChunkLog() throws Exception {
        BufferedWriter w = Files.newBufferedWriter(
                runDir.resolve("chunks.csv"),
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE
        );
        w.write("worker,chunk_id,lines,processing_time_ms");
        w.newLine();
        w.close();
    }

    private void writeChunkLog(
            String worker, int chunkId, int lines, long timeMs
    ) {
        try (BufferedWriter w = Files.newBufferedWriter(
                runDir.resolve("chunks.csv"),
                StandardOpenOption.APPEND)) {

            w.write(worker + "," + chunkId + "," + lines + "," + timeMs);
            w.newLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeFinalWordCount() {
        try (BufferedWriter w = Files.newBufferedWriter(
                runDir.resolve("wordcount.txt"))) {

            for (Map.Entry<String, Integer> e : finalWordCount.entrySet()) {
                w.write(e.getKey() + "=" + e.getValue());
                w.newLine();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void writeRunSummary(long totalTime) {
        try (BufferedWriter w = Files.newBufferedWriter(
                runDir.resolve("run_summary.txt"))) {

            w.write("Run ID: " + runId);
            w.newLine();
            w.write("Workers: " + aliveWorkers.size());
            w.newLine();
            w.write("Total lines: " + totalLinesCount);
            w.newLine();
            w.write("Total chunks: " + totalChunks);
            w.newLine();
            w.write("Total time (ms): " + totalTime);
            w.newLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= EVENT LOG =================

    private synchronized void logEvent(
            String eventType, String node, String details
    ) {
        try (BufferedWriter w = Files.newBufferedWriter(
                runDir.resolve("event_log.csv"),
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND)) {

            w.write(LocalDateTime.now() + "," + eventType + "," + node + "," +
                    (details == null ? "" : details));
            w.newLine();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
