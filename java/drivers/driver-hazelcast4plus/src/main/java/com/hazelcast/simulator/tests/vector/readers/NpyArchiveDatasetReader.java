package com.hazelcast.simulator.tests.vector.readers;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.tests.vector.DatasetReader;
import com.hazelcast.simulator.tests.vector.VectorUtils;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import org.apache.commons.io.FileUtils;
import org.jetbrains.bio.npy.NpyArray;
import org.jetbrains.bio.npy.NpyFile;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.file.Files.lines;

public class NpyArchiveDatasetReader extends DatasetReader {

    private Path trainDatasetFilename;
    private Path payloadsFilename;
    private Path testDatesetFilename;

    public NpyArchiveDatasetReader(String url, String directory, boolean normalizeVector, boolean testOnly) {
        super(url, directory, normalizeVector, testOnly);
    }

    @Override
    protected void preprocessDatasetFile() {
        this.trainDatasetFilename = Path.of(workingDirectory.toString(), "vectors.npy");
        this.payloadsFilename = Path.of(workingDirectory.toString(), "payloads.jsonl");
        this.testDatesetFilename = Path.of(workingDirectory.toString(), "tests.jsonl");

        if (!trainDatasetFilename.toFile().exists()) {
            unpack();
        }
        logger.info("Unpacking finished.");
    }

    private void unpack() {
        try {
            TarExtractor.extractTarGZ(new FileInputStream(downloadedFile), workingDirectory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void parseTrainDataset() {
        NpyArray read = NpyFile.read(trainDatasetFilename, Integer.MAX_VALUE);
        var shape = read.getShape();
        size = shape[0];
        dimension = shape[1];
        trainDataset = new float[size][];
        var trainDatasetPlain = read.asFloatArray();
        for (int i = 0; i < size; i++) {
            var vector = getTrainVectorPlain(i, trainDatasetPlain);
            if (normalizeVector) {
                VectorUtils.normalize(vector);
            }
            trainDataset[i] = vector;
        }

        if (payloadsFilename.toFile().exists()) {
            try (var lines = lines(payloadsFilename, Charset.defaultCharset())) {
                payloads = lines.map(HazelcastJsonValue::new).toArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected void parseTestDataset() {
        try {
            List<String> queryList = FileUtils.readLines(testDatesetFilename.toFile(), Charset.defaultCharset());
            int size = queryList.size();
            var searchVectors = new float[size][dimension];
            var searchClosestIds = new int[size][];
            var searchClosestScore = new float[size][];
            var searchConditions = payloads != null ? new Predicate[size] : null;
            for (int i = 0; i < queryList.size(); i++) {
                var queryObject = JsonParser.parseString(queryList.get(i)).getAsJsonObject();
                var jsonArray = queryObject.getAsJsonArray("query");
                var ids = queryObject.getAsJsonArray("closest_ids");
                var scores = queryObject.getAsJsonArray("closest_scores");
                searchVectors[i] = convertToFloatArray(jsonArray);
                if (normalizeVector) {
                    VectorUtils.normalize(searchVectors[i]);
                }
                searchClosestIds[i] = convertToIntArray(ids);
                searchClosestScore[i] = convertToFloatArray(scores);
                if (searchConditions != null) {
                    // filter condition require having payloads
                    var conditions = queryObject.getAsJsonObject("conditions");
                    searchConditions[i] = parseConditions(conditions);
                }
            }
            testDataset = new TestDataset(searchVectors, searchConditions, searchClosestIds, searchClosestScore);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Parses limited number of filter expressions used in the arxiv benchmark.
     * It is tailored to metadata columns in that benchmark.
     * Fails fast on unsupported predicate.
     */
    static Predicate<?, ?> parseConditions(JsonObject json) {
        if (json == null) {
            return null;
        }

        var topLevel = json.getAsJsonArray("and");
        if (topLevel == null) {
            throw new IllegalArgumentException("topLevel condition is expected to be 'and'");
        }
        List<Predicate<?, ?>> predicates = new ArrayList<>();
        topLevel.forEach(filterO -> {
            var filter = filterO.getAsJsonObject();
            if (filter.size() != 1) {
                throw new IllegalArgumentException("filter attributes must have exactly one element");
            }
            var fieldName = filter.keySet().stream().findFirst().orElseThrow();
            // collection field must use [any]
            // labels is arxiv-specific field
            var adjustedFieldName = "labels".equals(fieldName) ? fieldName + "[any]" : fieldName;

            var condition = filter.get(fieldName).getAsJsonObject();
            if (condition.size() != 1) {
                throw new IllegalArgumentException("filter conditions must have exactly one element");
            }
            var predicateType = condition.keySet().stream().findFirst().orElseThrow();
            var conditionValue = condition.get(predicateType).getAsJsonObject();
            var pred = switch (predicateType) {
                case "match" -> Predicates.equal(adjustedFieldName, conditionValue.getAsJsonPrimitive("value").getAsString());
                // arxiv benchmark has range only on numeric field
                case "range" -> Predicates.and(
                        Predicates.greaterThan(adjustedFieldName, conditionValue.getAsJsonPrimitive("gt").getAsInt()),
                        Predicates.lessThan(adjustedFieldName, conditionValue.getAsJsonPrimitive("lt").getAsInt())
                        );
                default -> throw new IllegalArgumentException("invalid predicate type " + predicateType);
            };

            predicates.add(pred);
        });

        return predicates.size() > 1
                ? Predicates.and(predicates.toArray(new Predicate[0]))
                : predicates.get(0);
    }


    private float[] convertToFloatArray(JsonArray array) {
        var result = new float[array.size()];
        for (int i = 0; i < array.size(); i++) {
            result[i] = array.get(i).getAsFloat();
        }
        return result;
    }

    private int[] convertToIntArray(JsonArray array) {
        var result = new int[array.size()];
        for (int i = 0; i < array.size(); i++) {
            result[i] = array.get(i).getAsInt();
        }
        return result;
    }

    private float[] getTrainVectorPlain(int index, float[] trainDatasetPlain) {
        if (index >= size) {
            throw new RuntimeException("invalid index");
        }
        return Arrays.copyOfRange(trainDatasetPlain, index * dimension, (index + 1) * dimension);
    }
}
