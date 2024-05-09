package com.hazelcast.simulator.tests.vector;

import com.hazelcast.vector.SearchResult;
import com.hazelcast.vector.SearchResults;
import com.hazelcast.config.vector.Metric;

import java.util.function.Consumer;
import java.util.function.Function;

public class VectorUtils {

    public static void forEach(SearchResults searchResults, Consumer<SearchResult> consumer) {
        var resultsIterator = searchResults.results();
        while (resultsIterator.hasNext()) {
            consumer.accept(resultsIterator.next());
        }
    }

    public static Function<Float, Float> restoreRealMetric(Metric metric) {
        return switch (metric) {
            case COSINE -> jMetric -> 2 * jMetric - 1;
            default -> jMetric -> -1f;
        };
    }
}
