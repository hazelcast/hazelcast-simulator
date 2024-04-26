package com.hazelcast.simulator.tests.vector.readers;

import com.hazelcast.simulator.tests.vector.DatasetReader;
import io.jhdf.HdfFile;
import io.jhdf.api.Dataset;

public class HDF5DatasetReader extends DatasetReader {

    private static int BULK_READER_SIZE = 50_000;

    public HDF5DatasetReader(String url, String directory) {
        super(url, directory);
    }

    @Override
    protected void preprocessDatasetFile() {

    }

    @Override
    protected void parseTrainDataset() {
        trainDataset = getDataset("train");
        size = trainDataset.length;
        dimension = trainDataset[0].length;
    }

    @Override
    protected void parseTestDataset() {
        testDataset = getDataset("test");
    }

    private float[][] getDataset(String datasetName) {
        try (HdfFile hdfFile = new HdfFile(downloadedFile.toPath())) {
            var datasetNode = hdfFile.getChildren().get(datasetName);
            Dataset dataset = hdfFile.getDatasetByPath(datasetNode.getPath());
            var dimension = dataset.getDimensions()[1];
            var size = dataset.getDimensions()[0];

            float[][] array = new float[size][dimension];

            for (int i = 0; i < size; i += BULK_READER_SIZE) {
                int length = Math.min(BULK_READER_SIZE, size - i);
                float[][] buffer = (float[][]) dataset.getData(new long[] {i, 0}, new int[]{length, dimension});
                System.arraycopy(buffer, 0, array, i, buffer.length);
            }
            return array;
        }
    }

}
