package com.hazelcast.simulator.probes;

import com.hazelcast.simulator.probes.impl.Result;
import org.HdrHistogram.Histogram;

import java.util.Random;

import static com.hazelcast.simulator.probes.impl.HdrProbe.HIGHEST_TRACKABLE_VALUE;

public final class ProbeTestUtils {

    private ProbeTestUtils() {
    }

    private static final int HISTOGRAM_RECORD_COUNT = 5000;
    private static final int MAX_LATENCY = 30000;

    private static final Random RANDOM = new Random();

    public static Result createProbeResult(int histogramCount) {
        Result result = new Result("test", histogramCount * HISTOGRAM_RECORD_COUNT, HISTOGRAM_RECORD_COUNT);
        for (int i = 1; i <= histogramCount; i++) {
            Histogram histogram = createRandomHistogram(HISTOGRAM_RECORD_COUNT);
            result.addHistogram("probe" + i, histogram);
        }
        return result;
    }

    public static Histogram createRandomHistogram(int recordCount) {
        Histogram histogram = new Histogram(HIGHEST_TRACKABLE_VALUE, 3);
        for (int record = 0; record < recordCount; record++) {
            histogram.recordValue(getRandomLatency());
        }
        return histogram;
    }

    public static int getRandomLatency() {
        return RANDOM.nextInt(MAX_LATENCY);
    }
}
