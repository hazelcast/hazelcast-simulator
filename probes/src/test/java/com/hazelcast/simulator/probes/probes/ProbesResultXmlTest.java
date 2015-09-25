package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HdrProbe;
import com.hazelcast.simulator.probes.probes.impl.HdrResult;
import com.hazelcast.simulator.probes.probes.impl.ThroughputResult;
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
        HdrResult originalResult = createHdrLatencyDistribution();
        resultMap.put("hdrLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("hdrLatency"));
    }

    @Test
    public void testHdrLatencyProbeResult_combined() throws Exception {
        HdrResult originalResult = createHdrLatencyDistribution();
        HdrResult anotherResult = createHdrLatencyDistribution();
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("hdrLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("hdrLatency"));
    }

    @Test
    public void testOperationsPerSecResult() throws Exception {
        ThroughputResult originalResult = new ThroughputResult(100000, 1234.5);
        resultMap.put("operationsPerSec", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("operationsPerSec"));
    }

    @Test
    public void testOperationsPerSecResult_combined() throws Exception {
        ThroughputResult originalResult = new ThroughputResult(100000, 1234.5);
        ThroughputResult anotherResult = new ThroughputResult(200000, 5432.1);
        originalResult = originalResult.combine(anotherResult);
        resultMap.put("operationsPerSec", originalResult);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, read.get("operationsPerSec"));
    }

    @Test
    public void multipleProbes() throws Exception {
        HdrResult result1 = createHdrLatencyDistribution();
        resultMap.put("result1", result1);

        ThroughputResult result2 = new ThroughputResult(100000, 1234.5);
        resultMap.put("result2", result2);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(result1, read.get("result1"));
        assertEquals(result2, read.get("result2"));
    }

    @Test
    public void write_toFile() throws FileNotFoundException {
        HdrResult originalResult = createHdrLatencyDistribution();
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
        ThroughputResult original = new ThroughputResult(100000, 1234.5);
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

    private static HdrResult createHdrLatencyDistribution() {
        Histogram histogram = new Histogram(HdrProbe.MAXIMUM_LATENCY, 4);
        for (int i = 0; i < LATENCY_RECORD_COUNT; i++) {
            histogram.recordValue(getRandomLatency());
        }
        return new HdrResult(histogram);
    }

    private static Map<String, Result> serializeAndDeserializeAgain(Map<String, Result> resultMap) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ProbesResultXmlWriter.write(resultMap, outputStream);

        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        return ProbesResultXmlReader.read(inputStream);
    }
}