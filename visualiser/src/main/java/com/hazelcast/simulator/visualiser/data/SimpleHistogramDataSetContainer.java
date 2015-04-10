package com.hazelcast.simulator.visualiser.data;

import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;

import java.lang.reflect.Field;
import java.util.List;

public class SimpleHistogramDataSetContainer extends SimpleHistogramDataset {

    private final List bins;

    private long maxLatency;

    public SimpleHistogramDataSetContainer(Comparable key) {
        super(key);
        try {
            Field binsField = SimpleHistogramDataset.class.getDeclaredField("bins");
            binsField.setAccessible(true);
            bins = (List) binsField.get(this);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void addBin(SimpleHistogramBin bin) {
        bins.add(bin);
    }

    public long getMaxLatency() {
        return maxLatency;
    }

    public void setMaxLatency(long maxLatency) {
        this.maxLatency = maxLatency;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (!super.equals(o)) return false;

        SimpleHistogramDataSetContainer that = (SimpleHistogramDataSetContainer) o;

        if (maxLatency != that.maxLatency) return false;
        if (bins != null ? !bins.equals(that.bins) : that.bins != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = bins != null ? bins.hashCode() : 0;
        result = 31 * result + (int) (maxLatency ^ (maxLatency >>> 32));
        return result;
    }
}
