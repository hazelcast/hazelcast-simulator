package com.hazelcast.simulator.tests.vector;

import com.google.common.primitives.Floats;
import com.google.gson.stream.JsonWriter;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.shaded.io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import com.hazelcast.shaded.io.github.jbellis.jvector.vector.types.VectorFloat;
import com.hazelcast.shaded.io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import com.hazelcast.vector.impl.storage.ArrayVectorProvider;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

public class TestDatasetGenerator {

    private final VectorSimilarityFunction fn = VectorSimilarityFunction.COSINE;
    private final VectorTypeSupport provider = ArrayVectorProvider.getInstance();

    boolean normalize = true;
    int searchSize = 1000;
    int k = 10;

    public static void main(String[] args) {
        new TestDatasetGenerator().run();
    }

    void run() {
        DatasetReader reader = DatasetReader.create(
                "https://storage.googleapis.com/ann-filtered-benchmark/datasets/dbpedia_openai_1M.tgz",
                "/home/frantisek/work/hz/project-x/semantic-cache/",
                normalize
        );

        VectorFloat<?>[] allVectors = new VectorFloat[reader.getSize()];
        for (int i = 0; i < reader.getSize(); i++) {
            allVectors[i] = provider.createFloatVector(reader.getTrainVector(i));
        }

        // If there are 1M vectors, searchSize = 10_000, there will be
        // 990_000 train vectors
        VectorFloat<?>[] trainVectors = new VectorFloat[reader.getSize() - searchSize];
        System.arraycopy(allVectors, 0, trainVectors, 0, allVectors.length - searchSize);

        // 10_000 searchVectors
        VectorFloat<?>[] searchVectors = new VectorFloat[searchSize];
        System.arraycopy(allVectors, allVectors.length - searchSize, searchVectors, 0, searchSize);

        // Keep the float[][] as well to write them to the test.jsonl file
        float[][] searchVectorsF = new float[searchSize][];
        System.arraycopy(reader.trainDataset, allVectors.length - searchSize, searchVectorsF, 0, searchSize);


        AtomicInteger counter = new AtomicInteger();
        int[][] closestIds = new int[searchSize][];
        float[][] closestScores = new float[searchSize][];
        IntStream.range(0, searchSize)
                .boxed()
                .parallel()
                .forEach(i -> {
                    VectorFloat<?> searchVector = searchVectors[i];

                    List<Tuple2<Integer, Float>> results = new ArrayList<>(trainVectors.length);
                    for (int j = 0; j < trainVectors.length; j++) {
                        float similarity = fn.compare(searchVector, trainVectors[j]);
                        results.add(tuple2(j, similarity));
                    }

                    results.sort(Comparator.comparing(Tuple2::f1, Comparator.reverseOrder()));
                    closestIds[i] = results.subList(0, k)
                            .stream()
                            .mapToInt(Tuple2::f0)
                            .toArray();

                    closestScores[i] = Floats.toArray(results.subList(0, k)
                            .stream()
                            .map(Tuple2::f1)
                            .toList());

                    System.out.println("Done " + counter.incrementAndGet());
                });
        System.out.println("Computed");

        TestDataset testDataset = new TestDataset(searchVectorsF, closestIds, closestScores);

        write("/home/frantisek/work/hz/project-x/semantic-cache/test.jsonl", testDataset);
    }

    public static void write(String path, TestDataset dataset) {
        try {
            FileWriter fileWriter = new FileWriter(path);
            for (int i = 0; i < dataset.size(); i++) {

                JsonWriter writer = new JsonWriter(fileWriter);
                writer.beginObject();
                writer.name("query");

                writer.beginArray();
                float[] vector = dataset.getSearchVector(i);
                for (int j = 0; j < dataset.getDimension(); j++) {
                    writer.value(vector[j]);
                }
                writer.endArray();

                writer.name("closest_ids");

                writer.beginArray();
                int[] closestIds = dataset.getClosestIds(i);
                for (int j = 0; j < closestIds.length; j++) {
                    writer.value(closestIds[j]);
                }
                writer.endArray();

                writer.name("closest_scores");
                writer.beginArray();
                float[] closestScores = dataset.getClosestScores(i);
                for (int j = 0; j < closestScores.length; j++) {
                    writer.value(closestScores[j]);
                }
                writer.endArray();
                writer.endObject();
                writer.flush();

                fileWriter.write('\n');
            }

            fileWriter.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

}
