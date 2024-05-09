package com.hazelcast.simulator.tests.vector.model;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDatasetDiffblueTest {
    @Test
    public void testGetPrecisionV1_score100() {
        var actual = (
                new TestDataset(
                        new float[][]{new float[]{0f}},
                        new int[][]{new int[]{1, 2, 3, 4}},
                        new float[][]{new float[]{0f}}
                )
        ).getPrecisionV1(List.of(1, 2), 0, 2);
        assertEquals(1, actual, 0.0f);
    }

    @Test
    public void testGetPrecisionV1_score0() {
        assertEquals(0.0f,
                (
                        new TestDataset(
                                new float[][]{new float[]{0f}},
                                new int[][]{new int[]{1, 2, 3, 4}, new int[]{1, 2, 1, 2}},
                                new float[][]{new float[]{0f}}
                        )
                ).getPrecisionV1(List.of(2), 0, 1),
                0.0f);
    }

    @Test
    public void testGetPrecisionV1_score50() {
        assertEquals(0.5f,
                (
                        new TestDataset(
                                new float[][]{new float[]{0f}},
                                new int[][]{new int[]{1, 2, 3, 4}, new int[]{2, 5, 6}},
                                new float[][]{new float[]{0f}}
                        )
                ).getPrecisionV1(List.of(2, 6), 0, 2),
                0.1f);
    }

    @Test
    public void testGetPrecisionV2_theSameVector() {
        var actual = (
                new TestDataset(
                        new float[][]{new float[]{0f}},
                        new int[][]{new int[]{1, 2, 3, 4}},
                        new float[][]{new float[]{10.0f, 0.5f, 10.0f, 0.5f}}
                )
        ).getPrecisionV2(new float[]{10.0f, 0.5f, 10.0f, 0.5f}, 0);
        assertEquals(0, actual, 0.0f);
    }

    @Test
    public void testGetPrecisionV2_DiffrentVector() {
        var actual = (
                new TestDataset(
                        new float[][]{new float[]{0f}},
                        new int[][]{new int[]{1, 2, 3, 4}},
                        new float[][]{new float[]{10.0f, 0.5f, 10.0f, 0.5f}}
                )
        ).getPrecisionV2(new float[]{8.0f, 0.4f, 9.0f, 0.5f}, 0);
        assertEquals(Math.sqrt(5.01), actual, 0.1f);
    }
}
