package com.hazelcast.simulator.utils;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import com.hazelcast.simulator.utils.OperationsFileAggregator.OperationState;
import com.hazelcast.simulator.utils.OperationsFileAggregator.OperationsOverTime;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static com.hazelcast.simulator.utils.OperationsFileAggregator.groupOperationsByTest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class OperationsFileAggregatorTest {

    @Test
    public void testOperationsParse()
            throws IOException {
        var fs = Jimfs.newFileSystem(Configuration.unix());
        var input = fs.getPath("operations.csv");
        Files.writeString(input, """
                epoch,timestamp,operations,operations-delta,operations/second
                1742469698.651,20/03/2025 11:21:38,5399,5399,5393.606
                1742469699.651,20/03/2025 11:21:39,11398,5999,5999
                1742469700.651,20/03/2025 11:21:40,16782,5384,5384
                """);

        assertThat(OperationsFileAggregator.parse(input), equalTo(new OperationsOverTime(
                List.of(new OperationState(1742469699, 5399, 5399, 5393.606), new OperationState(1742469700, 11398, 5999, 5999),
                        new OperationState(1742469701, 16782, 5384, 5384)))));
    }

    @Test
    public void testOperationGrouping()
            throws IOException {
        var fs = Jimfs.newFileSystem(Configuration.unix());
        var paths = Stream.of("run/A1_W1-x-member/operations.csv", "run/A2_W1-x-javaclient/operations.csv",
                                  "run/A2_W202-x-javaclient/operations.csv", "run/A2_W202-x-javaclient/operations123.csv",
                                  "run/A301_W1-y-javaclient/operationsxyz.csv", "run/A302_W2-y-javaclient/operations.csv").map(fs::getPath)
                          .toList();

        for (var p : paths) {
            Files.createDirectories(p.getParent());
            Files.createFile(p);
        }

        var expected = Map.of("", List.of(paths.get(1), paths.get(2), paths.get(5)), "123", List.of(paths.get(3)), "xyz",
                List.of(paths.get(4)));

        assertThat(groupOperationsByTest(fs.getPath("run")), equalTo(expected));
    }

    @Test
    public void testOperationCombining() {
        var opsA = new OperationsOverTime(
                List.of(new OperationState(100, 10000, 1000, 900.1), new OperationState(98, 5000, 900, 920.2),
                        new OperationState(101, 12000, 1200, 1100.1)));

        var opsB = new OperationsOverTime(
                List.of(new OperationState(100, 11000, 900, 800.1), new OperationState(99, 5100, 950, 820.2),
                        new OperationState(101, 13000, 1100, 1200.1)));

        var expected = new OperationsOverTime(
                List.of(new OperationState(98, 5000, 900, 920.2), new OperationState(99, 5100, 950, 820.2),
                        new OperationState(100, 21000, 1900, 1700.2), new OperationState(101, 25000, 2300, 2300.2)));

        assertThat(OperationsFileAggregator.combine(opsA, opsB), equalTo(expected));
    }

    @Test
    public void testWriteOutput()
            throws IOException {
        var fs = Jimfs.newFileSystem(Configuration.unix());
        var outputDest = fs.getPath("output.csv");
        OperationsFileAggregator.writeOutput(outputDest, new OperationsOverTime(
                List.of(new OperationState(1744210069, 1000, 1000, 999.23635),
                        new OperationState(1744210070, 1900, 900, 899.72534))));

        var expected = List.of(
                "epoch,timestamp,operations,operations-delta,operations/second",
                "1744210069,09/04/2025 14:47:49,1000,1000,999.2364",
                "1744210070,09/04/2025 14:47:50,1900,900,899.7253"
        );

        assertThat(Files.readAllLines(outputDest), equalTo(expected));
    }
}
