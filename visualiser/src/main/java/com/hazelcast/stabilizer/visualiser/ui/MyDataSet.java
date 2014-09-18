package com.hazelcast.stabilizer.visualiser.ui;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.IntervalXYDataset;

import java.util.ArrayList;
import java.util.List;

public class MyDataSet extends AbstractIntervalXYDataset implements IntervalXYDataset {
    private int noOfSeries = 0;
    private List<IntervalXYDataset> series = new ArrayList<IntervalXYDataset>();
    private List<String> keys = new ArrayList<String>();

    public void addNewSeries(IntervalXYDataset serie, String key) {
        noOfSeries++;
        series.add(serie);
        keys.add(key);
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
