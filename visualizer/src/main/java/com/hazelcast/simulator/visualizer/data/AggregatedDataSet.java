/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.visualizer.data;

import org.jfree.data.xy.AbstractIntervalXYDataset;
import org.jfree.data.xy.IntervalXYDataset;

import java.util.ArrayList;
import java.util.List;

public class AggregatedDataSet extends AbstractIntervalXYDataset implements IntervalXYDataset {

    private final List<IntervalXYDataset> series = new ArrayList<IntervalXYDataset>();
    private final List<String> keys = new ArrayList<String>();

    private long autoScaleValue;
    private int noOfSeries;

    public void addNewSeries(SimulatorHistogramDataSet dataSet, String key) {
        autoScaleValue = Math.max(autoScaleValue, dataSet.getAutoScaleValue());
        addNewSeries((IntervalXYDataset) dataSet, key);
    }

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

    public long getAutoScaleValue() {
        return autoScaleValue;
    }
}
