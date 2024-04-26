package com.hazelcast.simulator.tests.vector.readers;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.hazelcast.simulator.tests.vector.DatasetReader;
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

    private float[] trainDatasetPlain;

    public NpyArchiveDatasetReader(String url, String directory) {
        super(url, directory);
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
        trainDatasetPlain = read.asFloatArray();
    }

    @Override
    protected void parseTestDataset() {
        try {
            var parser = new JsonParser();
            List<String> queryList = FileUtils.readLines(testDatesetFilename.toFile(), Charset.defaultCharset());
            testDataset = new float[queryList.size()][dimension];
            for (int i = 0; i < queryList.size(); i++) {
                var jsonArray = parser.parse(queryList.get(i)).getAsJsonObject().getAsJsonArray("query");
                testDataset[i] = convert(jsonArray);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private float[] convert(JsonArray array) {
        var result = new float[array.size()];
        for (int i = 0; i < array.size(); i++) {
            result[i] = array.get(i).getAsFloat();
        }
        return result;
    }

    @Override
    public float[] getTrainVector(int index) {
        if (index >= size) {
            throw new RuntimeException("invalid index");
        }
        return Arrays.copyOfRange(trainDatasetPlain, index * dimension, (index + 1) * dimension);
    }
}
