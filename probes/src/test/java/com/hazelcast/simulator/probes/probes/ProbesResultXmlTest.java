package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrLatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.simulator.probes.probes.impl.MaxLatencyResult;
import com.hazelcast.simulator.probes.probes.impl.OperationsPerSecResult;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.ReflectionUtils.invokePrivateConstructor;
import static org.junit.Assert.assertEquals;

public class ProbesResultXmlTest {

    public static final int LATENCY_RECORD_COUNT = 5000;
    public static final int MAX_LATENCY = 30000;

    private static final Random random = new Random();

    private final Map<String, Result> resultMap = new HashMap<String, Result>();

    @Test
    public void constructorReader() throws Exception {
        invokePrivateConstructor(ProbesResultXmlReader.class);
    }

    @Test
    public void constructorWriter() throws Exception {
        invokePrivateConstructor(ProbesResultXmlWriter.class);
    }

    @Test
    public void testHdrLatencyProbeResult() throws Exception {
        HdrLatencyDistributionResult originalResult = createHdrLatencyDistribution();
        resultMap.put("hdrLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("hdrLatency"));
    }

    @Test
    public void testHdrLatencyProbeResult_combined() throws Exception {
        HdrLatencyDistributionResult originalResult = createHdrLatencyDistribution();
        HdrLatencyDistributionResult anotherResult = createHdrLatencyDistribution();
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("hdrLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("hdrLatency"));
    }

    @Test
    public void testMaxLatencyResult() throws Exception {
        MaxLatencyResult originalResult = new MaxLatencyResult(getRandomLatency());
        resultMap.put("maxLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("maxLatency"));
    }

    @Test
    public void testMaxLatencyResult_combined() throws Exception {
        MaxLatencyResult originalResult = new MaxLatencyResult(getRandomLatency());
        MaxLatencyResult anotherResult = new MaxLatencyResult(getRandomLatency());
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("maxLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("maxLatency"));
    }

    @Test
    public void testLatencyDistributionResult() throws Exception {
        LatencyDistributionResult originalResult = createLatencyDistribution();
        resultMap.put("latencyDistribution", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("latencyDistribution"));
    }

    @Test
    public void testLatencyDistributionResult_combined() throws Exception {
        LatencyDistributionResult originalResult = createLatencyDistribution();
        LatencyDistributionResult anotherResult = createLatencyDistribution();
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("latencyDistribution", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("latencyDistribution"));
    }

    @Test
    public void testOperationsPerSecResult() throws Exception {
        OperationsPerSecResult originalResult = new OperationsPerSecResult(100000, 1234.5);
        resultMap.put("operationsPerSec", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("operationsPerSec"));
    }

    @Test
    public void testOperationsPerSecResult_combined() throws Exception {
        OperationsPerSecResult originalResult = new OperationsPerSecResult(100000, 1234.5);
        OperationsPerSecResult anotherResult = new OperationsPerSecResult(200000, 5432.1);
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("operationsPerSec", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("operationsPerSec"));
    }

    @Test
    public void multipleProbes() throws Exception {
        HdrLatencyDistributionResult result1 = createHdrLatencyDistribution();
        resultMap.put("result1", result1);

        LatencyDistributionResult result2 = createLatencyDistribution();
        resultMap.put("result2", result2);

        MaxLatencyResult result3 = new MaxLatencyResult(Integer.MAX_VALUE);
        resultMap.put("result3", result3);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(result1, read.get("result1"));
        assertEquals(result2, read.get("result2"));
        assertEquals(result3, read.get("result3"));
    }

    @Test
    public void write_toFile() throws FileNotFoundException {
        HdrLatencyDistributionResult originalResult = createHdrLatencyDistribution();
        resultMap.put("hdrLatencyDistribution", originalResult);

        File tmpFile = new File("probes.xml");
        try {
            ensureExistingFile(tmpFile);
            ProbesResultXmlWriter.write(resultMap, tmpFile);

            InputStream targetStream = new FileInputStream(tmpFile);
            Map<String, Result> read = ProbesResultXmlReader.read(targetStream);

            assertEquals(originalResult, read.get("hdrLatencyDistribution"));
        } finally {
            deleteQuiet(tmpFile);
        }
    }

    @Test(expected = Exception.class)
    public void write_toDirectory() {
        LatencyDistributionResult original = createLatencyDistribution();
        resultMap.put("latencyDistribution", original);

        File tmpDirectory = new File("isDirectory");
        try {
            ensureExistingDirectory(tmpDirectory);
            ProbesResultXmlWriter.write(resultMap, tmpDirectory);
        } finally {
            deleteQuiet(tmpDirectory);
        }
    }

    private static int getRandomLatency() {
        return random.nextInt(MAX_LATENCY);
    }

    private static HdrLatencyDistributionResult createHdrLatencyDistribution() {
        Histogram histogram = new Histogram(HdrLatencyDistributionProbe.MAXIMUM_LATENCY, 4);
        for (int i = 0; i < LATENCY_RECORD_COUNT; i++) {
            histogram.recordValue(getRandomLatency());
        }
        return new HdrLatencyDistributionResult(histogram);
    }

    private static LatencyDistributionResult createLatencyDistribution() {
        LinearHistogram histogram = new LinearHistogram(100, 1);
        for (int i = 0; i < LATENCY_RECORD_COUNT; i++) {
            histogram.addValue(getRandomLatency());
        }
        return new LatencyDistributionResult(histogram);
    }

    private static Map<String, Result> serializeAndDeserializeAgain(Map<String, Result> resultMap) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ProbesResultXmlWriter.write(resultMap, outputStream);

        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        return ProbesResultXmlReader.read(inputStream);
    }
}