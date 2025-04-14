package com.hazelcast.simulator.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.runAsync;

public class BatchedHistogramLogProcessor {
    /**
     * Utility to process multiple {@link SimulatorHistogramLogProcessor} invocations
     * in parallel. A single path is expected as an arg which points to file where each
     * line is interpreted as an invocation of {@link SimulatorHistogramLogProcessor}
     * detailing the args to pass for that invocation.
     */
    public static void main(String[] args)
            throws IOException, InterruptedException {
        Path input = Path.of(args[0]);
        List<String[]> processorInvocations = Files.readAllLines(input).stream().map(line -> line.trim().split("\\s+")).toList();
        int threadCount = Math.max(2, Runtime.getRuntime().availableProcessors() - 1);
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        try {
            List<CompletableFuture<Void>> tasks = new ArrayList<>();
            for (var processorInvocation : processorInvocations) {
                tasks.add(runAsync(() -> {
                    try (SimulatorHistogramLogProcessor processor = new SimulatorHistogramLogProcessor(processorInvocation)) {
                        processor.run();
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }, executor));
            }
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }
}
