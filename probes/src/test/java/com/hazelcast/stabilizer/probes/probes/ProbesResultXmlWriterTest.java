package com.hazelcast.stabilizer.probes.probes;

import com.hazelcast.stabilizer.probes.probes.impl.HdrLatencyDistributionProbe;
import com.hazelcast.stabilizer.probes.probes.impl.HdrLatencyProbeResult;
import com.hazelcast.stabilizer.probes.probes.impl.LatencyDistributionProbe;
import com.hazelcast.stabilizer.probes.probes.impl.LatencyDistributionResult;
import com.hazelcast.stabilizer.probes.probes.impl.MaxLatencyResult;
import org.HdrHistogram.Histogram;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProbesResultXmlWriterTest {

    @Test
    public void testHdrLatencyProbeResult() throws Exception {
        Map<String, Result> resultMap = new HashMap<String, Result>();
        Histogram histogram = new Histogram(HdrLatencyDistributionProbe.MAXIMUM_LATENCY, 4);
        HdrLatencyProbeResult originalResult = new HdrLatencyProbeResult(histogram);
        resultMap.put("getLatency", originalResult);
        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);
        assertEquals(originalResult, result.get("getLatency"));
    }

    @Test
    public void testMaxLatencyResult() throws Exception {
        Map<String, Result> resultMap = new HashMap<String, Result>();
        MaxLatencyResult originalResult = new MaxLatencyResult(1);
        resultMap.put("getLatency", originalResult);

        Map<String, Result> result = serializeAndDeserializeAgain(resultMap);

        assertEquals(originalResult, result.get("getLatency"));

    }

    @Test
    public void testLatencyDistributionResult() throws Exception {
        Map<String, Result> resultMap = new HashMap<String, Result>();
        LatencyDistributionResult original = createLatencyDistribution();
        resultMap.put("latencyDistribution", original);

        Map<String, Result> read = serializeAndDeserializeAgain(resultMap);

        assertEquals(original, read.get("latencyDistribution"));
    }

    @Test
    public void testMultipleProbes() throws Exception {
        Map<String, Result> resultMap = new HashMap<String, Result>();
        LatencyDistributionResult result1 = createLatencyDistribution();
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



    private Map<String, Result> serializeAndDeserializeAgain(Map<String, Result> resultMap) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ProbesResultXmlWriter probesResultXmlWriter = new ProbesResultXmlWriter();
        probesResultXmlWriter.write(resultMap, outputStream);
        InputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        ProbesResultXmlReader reader = new ProbesResultXmlReader();
        return reader.read(inputStream);
    }

    private LatencyDistributionResult createLatencyDistribution() {
        LinearHistogram histogram = new LinearHistogram(100, 1);
        histogram.addValue(0);
        histogram.addValue(1);
        histogram.addValue(2);
        histogram.addValue(1);
        histogram.addValue(5);
        histogram.addValue(80);

        return new LatencyDistributionResult(histogram);
    }
}