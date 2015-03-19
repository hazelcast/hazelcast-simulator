package com.hazelcast.simulator.visualiser.data;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.IntervalXYDataset;

import java.util.ArrayList;
import java.util.List;

public class AggregatedDataSet extends AbstractIntervalXYDataset implements IntervalXYDataset {

    private final List<IntervalXYDataset> series = new ArrayList<IntervalXYDataset>();
    private final List<String> keys = new ArrayList<String>();

    private int noOfSeries = 0;

    public void addNewSeries(IntervalXYDataset dataSet, String key) {
        series.add(dataSet);
        keys.add(key);

        noOfSeries++;
    }

    @Override
    public int getSeriesCount() {
        return noOfSeries;
    }

    @Override
    public Comparable getSeriesKey(int series) {
        return keys.get(series);
    }

    @Override
    public int getItemCount(int series) {
        return this.series.get(series).getItemCount(series);
    }

    @Override
    public Number getX(int series, int item) {
        return this.series.get(series).getX(series, item);
    }

    @Override
    public Number getY(int series, int item) {
        return this.series.get(series).getY(series, item);
    }

    @Override
    public Number getStartX(int series, int item) {
        return this.series.get(series).getStartX(series, item);
    }

    @Override
    public Number getEndX(int series, int item) {
        return this.series.get(series).getEndX(series, item);
    }

    @Override
    public Number getStartY(int series, int item) {
        return this.series.get(series).getStartY(series, item);
    }

    @Override
    public Number getEndY(int series, int item) {
        return this.series.get(series).getEndY(series, item);
    }
}
