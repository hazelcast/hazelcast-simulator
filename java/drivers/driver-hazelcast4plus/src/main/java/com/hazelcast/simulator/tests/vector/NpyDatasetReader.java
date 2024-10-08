package com.hazelcast.simulator.tests.vector;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;
import com.hazelcast.simulator.tests.vector.readers.TarExtractor;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.bio.npy.NpyArray;
import org.jetbrains.bio.npy.NpyFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

@Deprecated
public class NpyDatasetReader {

    private final URL datasetURL;

    private final Path workingDirectory;

    private final File downloadedFile;

    private final Path vectorsNameFile;
    private final Path testNameFile;

    private float[] vectorsPlain;

    private List<float[]> query;

    private int dimension;
    private int size;

    protected final Logger logger = LogManager.getLogger(getClass());

    public NpyDatasetReader(String url, String directory) {
        try {
            this.datasetURL = URI.create(url).toURL();
            this.workingDirectory = Path.of(directory, FilenameUtils.getBaseName(datasetURL.getFile()));
            this.downloadedFile = Path.of(workingDirectory.toString(), FilenameUtils.getName(datasetURL.getFile())).toFile();
            this.vectorsNameFile = Path.of(workingDirectory.toString(), "vectors.npy");
            this.testNameFile = Path.of(workingDirectory.toString(), "tests.jsonl");

            logger.info("Start downloading file from " + datasetURL);
            if (!downloadedFile.exists()) {
                download();
            }
            logger.info("File downloaded to " + downloadedFile + ". Start unpacking...");

            if (!vectorsNameFile.toFile().exists()) {
                unpack();
            }
            logger.info("Unpacking finished. Start parse vectors...");

            parseArray();
            parseTestCases();
            logger.info("Dataset reader is initialized");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void download() {
        try {
            FileUtils.copyURLToFile(
                    datasetURL,
                    downloadedFile,
                    120_000,
                    60_000 * 60);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void unpack() throws IOException {
        TarExtractor.extractTarGZ(new FileInputStream(downloadedFile), workingDirectory);
    }

    private void parseArray() {
        NpyArray read = NpyFile.read(vectorsNameFile, Integer.MAX_VALUE);
        var shape = read.getShape();
        size = shape[0];
        dimension = shape[1];
        vectorsPlain = read.asFloatArray();
    }

    private void parseTestCases() {
        try {
            var parser = new JsonParser();
            List<String> queryList = FileUtils.readLines(testNameFile.toFile(), Charset.defaultCharset());
            query = queryList.stream()
                    .map(query -> parser.parse(query).getAsJsonObject().getAsJsonArray("query"))
                    .map(this::convert)
                    .toList();
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

    private void cleanup() {
        try {
            FileUtils.cleanDirectory(workingDirectory.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public float[] read(int index) {
        if (index >= size) {
            throw new RuntimeException("invalid index");
        }
        return Arrays.copyOfRange(vectorsPlain, index * dimension, (index + 1) * dimension);
    }

    public List<float[]> getTestCases() {
        return query;
    }

    public int getDimension() {
        return dimension;
    }

    public int getSize() {
        return size;
    }

    public int getQueryDimension() {
        if(query.isEmpty()) {
            return 0;
        }
        return query.get(0).length;
    }
}
