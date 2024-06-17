package com.hazelcast.simulator.tests.vector;

import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.config.vector.Metric;

import java.util.function.Consumer;
import java.util.function.Function;

public class VectorUtils {

    public static void forEach(SearchResults<?, ?> searchResults, Consumer<SearchResult<?, ?>> consumer) {
        var resultsIterator = searchResults.results();
        while (resultsIterator.hasNext()) {
            consumer.accept(resultsIterator.next());
        }
    }

    public static Function<Float, Float> restoreRealMetric(Metric metric) {
        return switch (metric) {
            case COSINE, DOT -> jMetric -> 2 * jMetric - 1;
            default -> jMetric -> -1f;
        };
    }

    public static void normalize(float[] vector) {
        double length = 0;
        for (float v : vector) {
            length += v * v;
        }
        var scale = (float) (1 / Math.sqrt(length));
        for (int i = 0; i < vector.length; i++) {
            vector[i] *= scale;
        }
    }
}
