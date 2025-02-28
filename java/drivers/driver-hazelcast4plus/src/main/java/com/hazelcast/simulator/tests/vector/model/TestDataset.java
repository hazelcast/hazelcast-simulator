package com.hazelcast.simulator.tests.vector.model;

import com.hazelcast.query.Predicate;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

public class TestDataset {

    private final float[][] searchVectors;

    private final Predicate[] searchConditions;

    private final int[][] closestIds;

    private final float[][] closestScores;

    public TestDataset(float[][] searchVector, Predicate[] searchConditions, int[][] closestIds, float[][] closestScores) {
        this.searchVectors = searchVector;
        this.searchConditions = searchConditions;
        this.closestIds = closestIds;
        this.closestScores = closestScores;
    }

    public TestDataset(float[][] searchVectors, int[][] ids, float[][] scores) {
        this(searchVectors, null, ids, scores);
    }

    public float[] getSearchVector(int index) {
        return searchVectors[index];
    }

    public int getLimit(int index) {
        return closestIds[index].length;
    }

    public Predicate<?, ?> getSearchConditions(int index) {
        return searchConditions != null ? searchConditions[index] : null;
    }

    public int getDimension() {
        if(searchVectors.length == 0) {
            return 0;
        }
        return searchVectors[0].length;
    }

    public int size() {
        return searchVectors.length;
    }

    public float getPrecision(List<Integer> actualVectorsIds, int index, int top) {
        int[] closestIds = this.closestIds[index];
        return getPrecision(actualVectorsIds, top, closestIds);
    }

    public static float getPrecision(List<Integer> actualVectorsIds, int top, int[] closestIds) {
        var actualSet = new HashSet<>(actualVectorsIds);
        var expectedSet = Arrays.stream(Arrays.copyOfRange(closestIds, 0, top)).boxed().collect(Collectors.toSet());
        actualSet.retainAll(expectedSet);
        return ((float) actualSet.size()) / top;
    }
}
