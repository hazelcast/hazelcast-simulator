package com.hazelcast.simulator.tests.vector;

import org.HdrHistogram.Histogram;

public class ScoreMetrics {

    private static final Histogram scoreHistogram = new Histogram(100, 3);

    public static void set(int score) {
        scoreHistogram.recordValue(score);
    }

    public static long getMin() {
        return scoreHistogram.getMinValue();
    }

    public static long getMax() {
        return scoreHistogram.getMaxValue();
    }

    public static double getMean() {
        return scoreHistogram.getMean();
    }

    public static long getPercentLowerThen(int value) {
        var lower = scoreHistogram.getCountBetweenValues(0, value);
        return (lower * 100) / scoreHistogram.getTotalCount();
    }
}
