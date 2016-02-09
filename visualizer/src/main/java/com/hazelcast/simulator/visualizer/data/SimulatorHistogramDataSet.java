/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;

import java.lang.reflect.Field;
import java.util.List;

public class SimulatorHistogramDataSet extends SimpleHistogramDataset {

    private final List bins;

    private long autoScaleValue;

    public SimulatorHistogramDataSet(Comparable key) {
        super(key);
        try {
            Field binsField = SimpleHistogramDataset.class.getDeclaredField("bins");
            binsField.setAccessible(true);
            bins = (List) binsField.get(this);
        } catch (NoSuchFieldException e) {
            throw new SimulatorHistogramDataSetException(e);
        } catch (IllegalAccessException e) {
            throw new SimulatorHistogramDataSetException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addBin(SimpleHistogramBin bin) {
        bins.add(bin);
    }

    public long getAutoScaleValue() {
        return autoScaleValue;
    }

    public void setAutoScaleValue(long autoScaleValue) {
        this.autoScaleValue = autoScaleValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        SimulatorHistogramDataSet that = (SimulatorHistogramDataSet) o;
        if (autoScaleValue != that.autoScaleValue) {
            return false;
        }
        return !(bins != null ? !bins.equals(that.bins) : that.bins != null);
    }

    @Override
    public int hashCode() {
        int result = bins != null ? bins.hashCode() : 0;
        result = 31 * result + (int) (autoScaleValue ^ (autoScaleValue >>> 32));
        return result;
    }
}
