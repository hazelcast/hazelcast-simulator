package com.hazelcast.simulator.probes.probes.utils;

import com.hazelcast.simulator.probes.probes.ProbeXmlUtils;
import com.hazelcast.simulator.probes.probes.Result;
import com.hazelcast.simulator.probes.probes.impl.ResultImpl;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.util.Random;

import static com.hazelcast.simulator.probes.probes.impl.ProbeImpl.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.probes.impl.ProbeImpl.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertEquals;

public final class ResultTestUtils {

    private ResultTestUtils() {
    }

    private static final int HISTOGRAM_RECORD_COUNT = 5000;
    private static final int MAX_LATENCY = 30000;

    private static final Random RANDOM = new Random();
    private static File resultFile = new File("tmpProbeResult.xml");

    public static void cleanup() {
        deleteQuiet(resultFile);
    }

    public static File getResultFile() {
        return resultFile;
    }

    public static Result createProbeResult(int histogramCount) {
        Result result = new ResultImpl("test", histogramCount * HISTOGRAM_RECORD_COUNT, HISTOGRAM_RECORD_COUNT);
        for (int i = 1; i <= histogramCount; i++) {
            Histogram histogram = createRandomHistogram(HISTOGRAM_RECORD_COUNT);
            result.addHistogram("probe" + i, histogram);
        }
        return result;
    }

    public static Histogram createRandomHistogram(int recordCount) {
        Histogram histogram = new Histogram(MAXIMUM_LATENCY, LATENCY_PRECISION);
        for (int record = 0; record < recordCount; record++) {
            histogram.recordValue(getRandomLatency());
        }
        return histogram;
    }

    public static Result serializeAndDeserializeAgain(Result result) {
        ProbeXmlUtils.toXml(result, resultFile);
        return ProbeXmlUtils.fromXml(resultFile);
    }

    public static int getRandomLatency() {
        return RANDOM.nextInt(MAX_LATENCY);
    }

    public static void assertEqualsResult(Result firstResult, Result secondResult) {
        assertEquals(firstResult.getInvocations(), secondResult.getInvocations());
        assertEquals(firstResult.getThroughput(), secondResult.getThroughput(), 0.0001);
        assertEquals(firstResult.probeNames(), secondResult.probeNames());

        for (String probeName : firstResult.probeNames()) {
            Histogram firstHistogram = firstResult.getHistogram(probeName);
            Histogram secondHistogram = secondResult.getHistogram(probeName);
            assertEquals(firstHistogram, secondHistogram);
        }
    }
}
