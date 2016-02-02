package com.hazelcast.simulator.probes;

import com.hazelcast.simulator.probes.impl.ResultImpl;
import com.hazelcast.simulator.probes.xml.ResultXmlUtils;
import org.HdrHistogram.Histogram;

import java.io.File;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.probes.impl.ProbeImpl.LATENCY_PRECISION;
import static com.hazelcast.simulator.probes.impl.ProbeImpl.MAXIMUM_LATENCY;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.TestUtils.assertEqualsStringFormat;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public final class ProbeTestUtils {

    private ProbeTestUtils() {
    }

    private static final int HISTOGRAM_RECORD_COUNT = 5000;
    private static final int MAX_LATENCY = 30000;
    private static final int TOLERANCE_MILLIS = 1000;

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
        ResultXmlUtils.toXml(result, resultFile);
        return ResultXmlUtils.fromXml(resultFile);
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

    public static void assertHistogram(Histogram histogram, long expectedCount, long expectedMinValueMillis,
                                       long expectedMaxValueMillis, long expectedMeanValueMillis) {
        long toleranceMicros = TimeUnit.MILLISECONDS.toMicros(TOLERANCE_MILLIS);

        long minValue = histogram.getMinValue();
        long maxValue = histogram.getMaxValue();
        assertNotEquals("Expected minValue and maxValue to differ", minValue, maxValue);
        assertWithinTolerance("minValue", TimeUnit.MILLISECONDS.toMicros(expectedMinValueMillis), minValue, toleranceMicros);
        assertWithinTolerance("maxValue", TimeUnit.MILLISECONDS.toMicros(expectedMaxValueMillis), maxValue, toleranceMicros);

        long meanValue = (long) histogram.getMean();
        assertWithinTolerance("meanValue", TimeUnit.MILLISECONDS.toMicros(expectedMeanValueMillis), meanValue, toleranceMicros);

        assertEqualsStringFormat("Expected %d records, but was %d", expectedCount, histogram.getTotalCount());
    }

    public static void assertWithinTolerance(String fieldName, long expected, long actual, long tolerance) {
        assertTrue(format("Expected %s >= %d, but was %d", fieldName, expected - tolerance, actual),
                actual >= expected - tolerance);
        assertTrue(format("Expected %s <= %d, but was %d", fieldName, expected + tolerance, actual),
                actual <= expected + tolerance);
    }
}
