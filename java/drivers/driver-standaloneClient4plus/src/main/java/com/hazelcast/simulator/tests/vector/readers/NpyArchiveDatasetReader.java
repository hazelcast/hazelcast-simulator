package com.hazelcast.simulator.tests.vector.readers;

import com.google.common.collect.Lists;
import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
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
import java.util.Arrays;
import java.util.List;

public class NpyArchiveDatasetReader extends DatasetReader {

    private Path trainDatasetFilename;
    private Path testDatesetFilename;

    public NpyArchiveDatasetReader(String url, String directory, boolean normalizeVector, boolean testOnly) {
        super(url, directory, normalizeVector, testOnly);
    }

    @Override
    protected void preprocessDatasetFile() {
        this.trainDatasetFilename = Path.of(workingDirectory.toString(), "vectors.npy");
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
    }

    @Override
    protected void parseTestDataset() {
        try {
            var parser = new JsonParser();
            List<String> queryList = FileUtils.readLines(testDatesetFilename.toFile(), Charset.defaultCharset());
            int size = queryList.size();
            var searchVectors = new float[size][dimension];
            var searchClosestIds = new int[size][];
            var searchClosestScore = new float[size][];
            for (int i = 0; i < queryList.size(); i++) {
                var queryObject = parser.parse(queryList.get(i)).getAsJsonObject();
                var jsonArray = queryObject.getAsJsonArray("query");
                var ids = queryObject.getAsJsonArray("closest_ids");
                var scores = queryObject.getAsJsonArray("closest_scores");
                searchVectors[i] = convertToFloatArray(jsonArray);
                if (normalizeVector) {
                    VectorUtils.normalize(searchVectors[i]);
                }
                searchClosestIds[i] = convertToIntArray(ids);
                searchClosestScore[i] = convertToFloatArray(scores);
            }
            testDataset = new TestDataset(searchVectors, searchClosestIds, searchClosestScore);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
