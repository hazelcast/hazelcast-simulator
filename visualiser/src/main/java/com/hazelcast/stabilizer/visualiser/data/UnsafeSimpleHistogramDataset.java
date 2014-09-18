package com.hazelcast.stabilizer.visualiser.data;

import org.jfree.data.statistics.SimpleHistogramBin;
import org.jfree.data.statistics.SimpleHistogramDataset;

import java.lang.reflect.Field;
import java.util.List;

public class UnsafeSimpleHistogramDataset extends SimpleHistogramDataset {
    private final Field binsField;
    private final List bins;

    public UnsafeSimpleHistogramDataset(Comparable key) {
        super(key);
        try {
            binsField = SimpleHistogramDataset.class.getDeclaredField("bins");
            binsField.setAccessible(true);
            bins = (List) binsField.get(this);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void addBin(SimpleHistogramBin bin) {
        bins.add(bin);
//        super.addBin(bin);
    }
}
