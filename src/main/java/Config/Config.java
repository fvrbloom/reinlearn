package config;

import java.io.*;
import java.util.*;

public class Config {

    public enum SchedulingPolicy {
        RL,
        ROUND_ROBIN,
        SHORTEST_QUEUE
    }

    // Platform config
    public String mainHost;
    public int mainPort;
    public List<String> workerAddresses; // format: "host:port/worker1"

    // Experiment config
    public SchedulingPolicy policy;
    public int numRuns;
    public String inputFile;
    public int maxChunkLines;

    // RL hyperparameters
    public double alpha;
    public double gamma;
    public double epsilon;

    public static Config load(String path) throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(path));

        Config cfg = new Config();

        // Platform
        cfg.mainHost = props.getProperty("main.host", "localhost");
        cfg.mainPort = Integer.parseInt(props.getProperty("main.port", "1099"));

        String workers = props.getProperty("worker.addresses");
        cfg.workerAddresses = Arrays.asList(workers.split(";"));

        // Experiment
        cfg.policy = SchedulingPolicy.valueOf(props.getProperty("scheduling.policy", "RL"));
        cfg.numRuns = Integer.parseInt(props.getProperty("experiment.num_runs", "5"));
        cfg.inputFile = props.getProperty("input.file", "big2.txt");
        cfg.maxChunkLines = Integer.parseInt(props.getProperty("chunk.max_lines", "20000"));

        // RL
        cfg.alpha = Double.parseDouble(props.getProperty("rl.alpha", "0.1"));
        cfg.gamma = Double.parseDouble(props.getProperty("rl.gamma", "0.9"));
        cfg.epsilon = Double.parseDouble(props.getProperty("rl.epsilon", "0.3"));

        return cfg;
    }

    public int getNumWorkers() {
        return workerAddresses.size();
    }
}