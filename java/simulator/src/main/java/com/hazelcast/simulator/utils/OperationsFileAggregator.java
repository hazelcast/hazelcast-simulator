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

import static java.math.RoundingMode.HALF_UP;
import static java.time.ZoneOffset.UTC;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.groupingBy;

public class OperationsFileAggregator {

    private static final DateTimeFormatter TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("dd/MM/yyyy hh:mm:ss");
    private static final Logger LOGGER = LogManager.getLogger(OperationsFileAggregator.class);

    record OperationState(long epoch, long operations, long operationsDelta, double operationsRate) {
        public OperationState(CSVRecord record) {
            this(Math.round(Double.parseDouble(record.get("epoch"))), Long.parseLong(record.get("operations")),
                    Long.parseLong(record.get("operations-delta")), Double.parseDouble(record.get("operations/second")));
        }

        public OperationState add(OperationState other) {
            return new OperationState(
                    epoch,
                    operations + other.operations,
                    operationsDelta + other.operationsDelta,
                    operationsRate + other.operationsRate
            );
        }
    }

    record OperationsOverTime(List<OperationState> states) {
    }

    static Map<String, List<Path>> separateOperationsByTest(Path runDir)
            throws IOException {
        try (var fileTree = Files.walk(runDir, 2)) {
            return fileTree.filter(Files::isRegularFile).filter(p -> p.getParent() != null)
                           .filter(p -> p.getParent().getFileName().toString().matches("^A\\d+_W\\d+-.*-javaclient$"))
                           .filter(p -> p.getFileName().toString().matches("^operations.*\\.csv$"))
                           .collect(groupingBy(p -> p.getFileName().toString().replace("operations", "").replace(".csv", "")));
        }
    }

    static OperationsOverTime parse(Path operations) {
        var format = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).get();
        Map<Long, OperationState> states = new HashMap<>();
        try (var parser = format.parse(Files.newBufferedReader(operations))) {
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
        var format = CSVFormat.DEFAULT.builder().setHeader("epoch", "timestamp", "operations", "operations-delta", "operations/second").get();
        try (var printer = format.print(dest, StandardCharsets.UTF_8)) {
            for (var state : operations.states) {
                printer.printRecord(
                        state.epoch,
                        Instant.ofEpochSecond(state.epoch).atZone(UTC).format(TIMESTAMP_FORMATTER),
                        state.operations,
                        state.operationsDelta,
                        new BigDecimal(state.operationsRate).setScale(4, HALF_UP).stripTrailingZeros().toPlainString()
                );
            }
        }
    }

    public static void main(String[] args)
            throws IOException {
        if (args.length != 1) {
            throw new IllegalArgumentException("Expects exactly one path argument pointing to the run directory");
        }
        Path runDir = Path.of(args[0]);
        if (!Files.isDirectory(runDir)) {
            throw new IllegalArgumentException(runDir + " is not a valid directory!");
        }
        Map<String, List<Path>> operationsByTest = separateOperationsByTest(runDir);
        for (var entry : operationsByTest.entrySet()) {
            String testId = entry.getKey();
            List<Path> workerOperations = entry.getValue();
            LOGGER.info("Combining {} files for testId \"{}\"", workerOperations.size(), testId);
            OperationsOverTime combined = workerOperations.stream().parallel().map(OperationsFileAggregator::parse)
                                                          .reduce(OperationsFileAggregator::combine).orElseThrow();
            var outputDest = runDir.resolve("operations" + testId + ".csv");
            LOGGER.info("Writing combined operations for testId \"{}\" to {}", testId, outputDest);
            writeOutput(outputDest, combined);
        }
    }
}
