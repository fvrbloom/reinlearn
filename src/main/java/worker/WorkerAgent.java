package worker;

import jade.core.Agent;
import jade.core.AID;
import jade.core.behaviours.CyclicBehaviour;
import jade.lang.acl.ACLMessage;

import java.util.HashMap;
import java.util.Map;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.io.BufferedWriter;

public class WorkerAgent extends Agent {

    private String masterName;
    private String runId;
    private Path runDir;

    @Override
    protected void setup() {

        // ==== ARGS ====
        // args[0] = master name
        // args[1] = runId
        Object[] args = getArguments();

        if (args == null || args.length < 2) {
            System.err.println(
                    getLocalName() +
                            " ERROR: expected args (masterName, runId)"
            );
            doDelete();
            return;
        }

        masterName = (String) args[0];
        runId = (String) args[1];

        runDir = Paths.get(
                System.getProperty("user.home"),
                "results",
                runId
        );

        try {
            Files.createDirectories(runDir);
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println(getLocalName() + " ready (run=" + runId + ")");
        logEvent("WORKER_STARTED", getLocalName(), null);

        // ==== HELLO MASTER ====
        ACLMessage hello = new ACLMessage(ACLMessage.INFORM);
        hello.addReceiver(new AID(masterName, AID.ISLOCALNAME));
        hello.setContent("HELLO");
        send(hello);

        addBehaviour(new CyclicBehaviour() {
            @Override
            public void action() {

                ACLMessage msg = receive();
                if (msg == null) {
                    block();
                    return;
                }

                logEvent("CHUNK_RECEIVED", getLocalName(), null);

                long start = System.currentTimeMillis();

                String chunk = msg.getContent();
                String[] lines = chunk.split("\n");

                Map<String, Integer> map = new HashMap<>();
                long processingTime;

                try {
                    for (String line : lines) {
                        for (String word : line.split("\\s+")) {

                            // normalize
                            word = word.toLowerCase()
                                    .replaceAll("[^a-z]", "");

                            if (word.isEmpty()) continue;

                            map.merge(word, 1, Integer::sum);
                        }
                    }

                    processingTime =
                            System.currentTimeMillis() - start;

                } catch (OutOfMemoryError oom) {

                    logEvent(
                            "OOM_DETECTED",
                            getLocalName(),
                            "heap_limit_mb=64"
                    );

                    System.err.println(
                            getLocalName() + " OOM detected"
                    );

                    System.gc();
                    return;
                }

                logEvent(
                        "CHUNK_PROCESSING_FINISHED",
                        getLocalName(),
                        "duration_ms=" + processingTime
                );

                String payload =
                        "CHUNK_TIME=" + processingTime + "\n" +
                                "LINES=" + lines.length + "\n" +
                                "DATA=" + map.toString();

                ACLMessage reply = msg.createReply();
                reply.setPerformative(ACLMessage.INFORM);
                reply.setContent(payload);

                logEvent("RESULT_SENT", getLocalName(), null);
                send(reply);

                System.out.println(
                        getLocalName() +
                                " processed chunk in " +
                                processingTime + " ms"
                );
            }
        });
    }

    // ===== EVENT LOG =====
    private synchronized void logEvent(
            String eventType,
            String node,
            String details
    ) {
        try {
            Path file = runDir.resolve("event_log.csv");
            boolean exists = Files.exists(file);

            BufferedWriter w = Files.newBufferedWriter(
                    file,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND
            );

            if (!exists) {
                w.write("timestamp,event_type,node,details");
                w.newLine();
            }

            w.write(
                    java.time.LocalDateTime.now() + "," +
                            eventType + "," +
                            node + "," +
                            (details == null ? "" :
                                    "\"" + details.replace("\"", "'") + "\"")
            );
            w.newLine();
            w.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
