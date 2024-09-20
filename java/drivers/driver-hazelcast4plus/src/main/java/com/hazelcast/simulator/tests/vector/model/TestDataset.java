package com.hazelcast.simulator.tests.vector.model;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class TestDataset {

    private final float[][] searchVectors;

    private final int[][] closestIds;

    private final float[][] closestScores;

    public TestDataset(float[][] searchVector, int[][] closestIds, float[][] closestScores) {
        this.searchVectors = searchVector;
        this.closestIds = closestIds;
        this.closestScores = closestScores;
    }

    public float[] getSearchVector(int index) {
        return searchVectors[index];
    }

    public int getDimension() {
        if(searchVectors.length == 0) {
            return 0;
        }
        return searchVectors[0].length;
    }

    public int[] getClosestIds(int i) {
        return closestIds[i];
    }

    public float[] getClosestScores(int i) {
        return closestScores[i];
    }

    public int size() {
        return searchVectors.length;
    }

    public float getPrecision(List<Integer> actualVectorsIds, int index, int top) {
        var actualSet = new HashSet<>(actualVectorsIds);
        var expectedSet = Arrays.stream(Arrays.copyOfRange(closestIds[index % closestIds.length], 0, top)).boxed().collect(Collectors.toSet());
        actualSet.retainAll(expectedSet);
        return ((float) actualSet.size()) / top;
    }

}
