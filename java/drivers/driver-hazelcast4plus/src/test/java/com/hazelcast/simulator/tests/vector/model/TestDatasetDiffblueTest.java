package com.hazelcast.simulator.tests.vector.model;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDatasetDiffblueTest {
    @Test
    public void testGetPrecision_score100() {
        var actual = (
                new TestDataset(
                        new float[][]{new float[]{0f}},
                        new int[][]{new int[]{1, 2, 3, 4}},
                        new float[][]{new float[]{0f}}
                )
        ).getPrecision(List.of(1, 2), 0, 2);
        assertEquals(1, actual, 0.0f);
    }

    @Test
    public void testGetPrecision_score0() {
        assertEquals(0.0f,
                (
                        new TestDataset(
                                new float[][]{new float[]{0f}},
                                new int[][]{new int[]{1, 2, 3, 4}, new int[]{1, 2, 1, 2}},
                                new float[][]{new float[]{0f}}
                        )
                ).getPrecision(List.of(2), 0, 1),
                0.0f);
    }

    @Test
    public void testGetPrecision_score50() {
        assertEquals(0.5f,
                (
                        new TestDataset(
                                new float[][]{new float[]{0f}},
                                new int[][]{new int[]{1, 2, 3, 4}, new int[]{2, 5, 6}},
                                new float[][]{new float[]{0f}}
                        )
                ).getPrecision(List.of(2, 6), 0, 2),
                0.1f);
    }
}
