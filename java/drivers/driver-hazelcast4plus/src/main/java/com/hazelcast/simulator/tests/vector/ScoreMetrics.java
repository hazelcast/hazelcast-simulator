package com.hazelcast.simulator.tests.vector;

import org.HdrHistogram.Histogram;

public class ScoreMetrics {

    private String name;

    private final Histogram scoreHistogram = new Histogram(100, 3);

    public ScoreMetrics() {
    }

    public void set(int score) {
        scoreHistogram.recordValue(score);
    }

    public long getMin() {
        return scoreHistogram.getMinValue();
    }

    public long getMax() {
        return scoreHistogram.getMaxValue();
    }

    public double getMean() {
        return scoreHistogram.getMean();
    }

    public double getPercentile(double value) {
        return scoreHistogram.getValueAtPercentile(value);
    }

    public long getPercentLowerThen(int value) {
        var lower = scoreHistogram.getCountBetweenValues(0, value);
        return (lower * 100) / scoreHistogram.getTotalCount();
    }

    public void setName(String name) {
        this.name = name;
    }
}
