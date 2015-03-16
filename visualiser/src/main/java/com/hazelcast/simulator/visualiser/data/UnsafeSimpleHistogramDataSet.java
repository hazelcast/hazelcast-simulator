package com.hazelcast.simulator.visualiser.data;

import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;

import java.lang.reflect.Field;
import java.util.List;

public class UnsafeSimpleHistogramDataSet extends SimpleHistogramDataset {

    private final List bins;

    public UnsafeSimpleHistogramDataSet(Comparable key) {
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
        //super.addBin(bin);
        bins.add(bin);
    }
}
