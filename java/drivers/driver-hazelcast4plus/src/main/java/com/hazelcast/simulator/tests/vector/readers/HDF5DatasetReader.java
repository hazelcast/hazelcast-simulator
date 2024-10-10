package com.hazelcast.simulator.tests.vector.readers;

import com.hazelcast.simulator.tests.vector.DatasetReader;
import com.hazelcast.simulator.tests.vector.VectorUtils;
import com.hazelcast.simulator.tests.vector.model.TestDataset;
import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;

public class HDF5DatasetReader extends DatasetReader {

    private static final int BULK_READER_SIZE = 50_000;

    public HDF5DatasetReader(String url, String directory, boolean normalizeVector, boolean testOnly) {
        super(url, directory, normalizeVector, testOnly);
    }

    @Override
    protected void preprocessDatasetFile() {

    }

    @Override
    protected void parseTrainDataset() {
        try (HdfFile hdfFile = new HdfFile(downloadedFile.toPath())) {
            trainDataset = getDatasetAsFloatMatrix(hdfFile, "train");
        }
        size = trainDataset.length;
        dimension = trainDataset[0].length;
        if (normalizeVector) {
            for (float[] vector : trainDataset) {
                VectorUtils.normalize(vector);
            }
        }
    }

    @Override
    protected void parseTestDataset() {
        try (HdfFile hdfFile = new HdfFile(downloadedFile.toPath())) {
            var searchVectors = getDatasetAsFloatMatrix(hdfFile, "test");
            var ids = getDatasetAsIntMatrix(hdfFile, "neighbors");
            var scores = getDatasetAsFloatMatrix(hdfFile, "distances");
            testDataset = new TestDataset(searchVectors, ids, scores);
            if (normalizeVector) {
                for (float[] vector : searchVectors) {
                    VectorUtils.normalize(vector);
                }
            }
        }
    }

    private float[][] getDatasetAsFloatMatrix(HdfFile hdfFile, String datasetName) {
        var datasetNode = hdfFile.getChildren().get(datasetName);
        Dataset dataset = hdfFile.getDatasetByPath(datasetNode.getPath());
        var dimension = dataset.getDimensions()[1];
        var size = dataset.getDimensions()[0];

        float[][] matrix = new float[size][dimension];

        for (int i = 0; i < size; i += BULK_READER_SIZE) {
            int length = Math.min(BULK_READER_SIZE, size - i);
            float[][] buffer = (float[][]) dataset.getData(new long[]{i, 0}, new int[]{length, dimension});
            System.arraycopy(buffer, 0, matrix, i, buffer.length);
        }
        return matrix;
    }

    private int[][] getDatasetAsIntMatrix(HdfFile hdfFile, String datasetName) {
        var datasetNode = hdfFile.getChildren().get(datasetName);
        Dataset dataset = hdfFile.getDatasetByPath(datasetNode.getPath());
        var dimension = dataset.getDimensions()[1];
        var size = dataset.getDimensions()[0];

        int[][] matrix = new int[size][dimension];

        for (int i = 0; i < size; i += BULK_READER_SIZE) {
            int length = Math.min(BULK_READER_SIZE, size - i);
            int[][] buffer = (int[][]) dataset.getData(new long[]{i, 0}, new int[]{length, dimension});
            System.arraycopy(buffer, 0, matrix, i, buffer.length);
        }
        return matrix;
    }
}
