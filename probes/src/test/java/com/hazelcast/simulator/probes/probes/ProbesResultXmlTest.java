package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ProbeImpl;
import com.hazelcast.simulator.probes.probes.impl.ResultImpl;
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
    public void testProbeResult() throws Exception {
        ResultImpl originalResult = createProbeResult();
        resultMap.put("probe", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("probe"));
    }

    @Test
    public void testProbeResult_combined() throws Exception {
        ResultImpl originalResult = createProbeResult();
        ResultImpl anotherResult = createProbeResult();
        originalResult = (ResultImpl) originalResult.combine(anotherResult);
        resultMap.put("probe", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("probe"));
    }

    @Test
    public void multipleProbes() throws Exception {
        ResultImpl result1 = createProbeResult();
        resultMap.put("result1", result1);

        ResultImpl result2 = createProbeResult();
        resultMap.put("result2", result2);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);
        assertEquals(result1, read.get("result1"));
        assertEquals(result2, read.get("result2"));
    }

    @Test
    public void write_toFile() throws FileNotFoundException {
        ResultImpl originalResult = createProbeResult();
        resultMap.put("latency", originalResult);

        File tmpFile = new File("probes.xml");
        try {
            ensureExistingFile(tmpFile);
            ProbesResultXmlWriter.write(resultMap, tmpFile);

            InputStream targetStream = new FileInputStream(tmpFile);
            Map<String, Result> read = ProbesResultXmlReader.read(targetStream);

            assertEquals(originalResult, read.get("latency"));
        } finally {
            deleteQuiet(tmpFile);
        }
    }

    @Test(expected = Exception.class)
    public void write_toDirectory() {
        ResultImpl original = createProbeResult();
        resultMap.put("latency", original);

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

    private static ResultImpl createProbeResult() {
        Histogram histogram = new Histogram(ProbeImpl.MAXIMUM_LATENCY, 4);
        for (int i = 0; i < LATENCY_RECORD_COUNT; i++) {
            histogram.recordValue(getRandomLatency());
        }
        return new ResultImpl(histogram, histogram.getTotalCount(), LATENCY_RECORD_COUNT);
    }

    private static Map<String, Result> serializeAndDeserializeAgain(Map<String, Result> resultMap) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ProbesResultXmlWriter.write(resultMap, outputStream);

        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        return ProbesResultXmlReader.read(inputStream);
    }
}