package com.hazelcast.simulator.utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toSet;

public class OperationsFileAggregator
        implements Runnable {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");
    private static final Logger LOGGER = LogManager.getLogger(OperationsFileAggregator.class);
    private static final int RATE_PRECISION = 4;
    private static final int WALK_DEPTH = 2;
    private static final CSVFormat.Builder CSV_COMMON_FORMAT = CSVFormat.DEFAULT.builder()
                                                                                .setHeader("epoch", "timestamp", "operations",
                                                                                        "operations-delta", "operations/second")
                                                                                .setRecordSeparator('\n');

    private final Path runDir;

    public OperationsFileAggregator(Path runDir) {
        this.runDir = runDir;
        if (!Files.isDirectory(runDir)) {
            throw new IllegalArgumentException(runDir + " is not a valid directory!");
        }
    }

    @Override
    public void run() {
        Map<String, Set<Path>> operationsByTest;
        try {
            operationsByTest = groupOperationsByTest(runDir);
        } catch (IOException e) {
            throw new RuntimeException("Unable to locate operations files from runDir=" + runDir, e);
        }

        for (var entry : operationsByTest.entrySet()) {
            String testId = entry.getKey();
            List<Path> workerOperations = entry.getValue().stream().toList();
            LOGGER.info("Combining {} files for testId \"{}\"", workerOperations.size(), testId);
            OperationsOverTime combined = workerOperations.stream().parallel().map(OperationsFileAggregator::parse)
                                                          .reduce(OperationsFileAggregator::combine).orElseThrow();
            var outputDest = runDir.resolve("operations" + testId + ".csv");
            LOGGER.info("Writing combined operations for testId \"{}\" to {}", testId, outputDest);
            try {
                writeOutput(outputDest, combined);
            } catch (IOException e) {
                throw new RuntimeException("Error writing combined operations to " + outputDest, e);
            }
        }
    }

    record OperationState(long epoch, long operations, long operationsDelta, double operationsRate) {
        public OperationState(CSVRecord record) {
            this(Math.round(Double.parseDouble(record.get("epoch"))), Long.parseLong(record.get("operations")),
                    Long.parseLong(record.get("operations-delta")), Double.parseDouble(record.get("operations/second")));
        }

        public OperationState add(OperationState other) {
            return new OperationState(epoch, operations + other.operations, operationsDelta + other.operationsDelta,
                    operationsRate + other.operationsRate);
        }
    }

    record OperationsOverTime(List<OperationState> states) {
    }

    static Map<String, Set<Path>> groupOperationsByTest(Path runDir)
            throws IOException {
        try (var fileTree = Files.walk(runDir, WALK_DEPTH)) {
            return fileTree.filter(Files::isRegularFile).filter(p -> p.getParent() != null)
                           .filter(p -> p.getParent().getFileName().toString().matches("^A\\d+_W\\d+-.*-javaclient$"))
                           .filter(p -> p.getFileName().toString().matches("^operations.*\\.csv$")).collect(
                            groupingBy(p -> p.getFileName().toString().replace("operations", "").replace(".csv", ""), toSet()));
        }
    }

    static OperationsOverTime parse(Path operations) {
        Map<Long, OperationState> states = new HashMap<>();
        try (var parser = CSV_COMMON_FORMAT.setSkipHeaderRecord(true).get().parse(Files.newBufferedReader(operations))) {
            parser.stream().map(OperationState::new).forEach(state -> states.put(state.epoch, state));
        } catch (IOException e) {
            throw new RuntimeException("Unable to parse " + operations.toAbsolutePath(), e);
        }
        return new OperationsOverTime(states.values().stream().sorted(comparingLong(OperationState::epoch)).toList());
    }

    static OperationsOverTime combine(OperationsOverTime a, OperationsOverTime b) {
        Map<Long, OperationState> states = new HashMap<>();
        a.states.forEach(state -> states.put(state.epoch, state));
        for (var state : b.states) {
            states.compute(state.epoch, (k, curr) -> curr == null ? state : curr.add(state));
        }
        return new OperationsOverTime(states.values().stream().sorted(comparingLong(OperationState::epoch)).toList());
    }

    static void writeOutput(Path dest, OperationsOverTime operations)
            throws IOException {
        try (var printer = CSV_COMMON_FORMAT.setSkipHeaderRecord(false).get().print(dest, StandardCharsets.UTF_8)) {
            for (var state : operations.states) {
                printer.printRecord(state.epoch, Instant.ofEpochSecond(state.epoch).atZone(UTC).format(TIMESTAMP_FORMATTER),
                        state.operations, state.operationsDelta,
                        new BigDecimal(state.operationsRate).setScale(RATE_PRECISION, HALF_UP).stripTrailingZeros()
                                                            .toPlainString());
            }
        }
    }

    public static void main(String[] args)
            throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expects exactly one path argument pointing to the run directory");
        }
        new OperationsFileAggregator(Path.of(args[0])).run();
    }
}
