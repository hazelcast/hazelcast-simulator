package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.tests.vector.DatasetReader;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Ignore("Manual tests")
public class DatasetReaderTest {

    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/random_keywords_1m.tgz";
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/hnm.tgz"; // work
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/yandex_t2i_gt_100k.tgz"; // not vectors in archive
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/laion-small-clip.tgz"; // broken?

    // downloaded dataset can be reused, adjust directory if needed
    private String workingDirectory = "/tmp/dataset_output/";

    @Test
    public void npy_dbpedia() {
        String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/dbpedia_openai_1M.tgz";

        DatasetReader reader = DatasetReader.create(url, workingDirectory, true);

        assertEquals(975_000, reader.getSize());
        assertEquals(1536, reader.getDimension());
        assertEquals(1536, reader.getTestDatasetDimension());

        assertNotNull(reader.getTrainVector(1234));
        assertEquals(5000, reader.getTestDataset().size());

        assertEquals(0.01739898, reader.getTrainVector(0)[0], 0.0001);
        assertEquals(-0.04525524, reader.getTrainVector(0)[1535], 0.0001);
    }

    @Test
    public void hdf5_angular() {
        String url = "http://ann-benchmarks.com/glove-100-angular.hdf5";

        DatasetReader reader = DatasetReader.create(url, workingDirectory, true);

        assertEquals(1_183_514, reader.getSize());
        assertEquals(100, reader.getDimension());
        assertEquals(100, reader.getTestDatasetDimension());

        assertEquals(10_000, reader.getTestDataset().size());
        assertEquals(-0.02701984, reader.getTrainVector(0)[0], 0.0001);
        assertEquals(-0.00503204, reader.getTrainVector(0)[99], 0.0001);

        assertEquals(0.08828659, reader.getTestDataset().getSearchVector(0)[0], 0.0001);
        assertEquals(-0.0329303, reader.getTestDataset().getSearchVector(0)[99], 0.0001);
    }
    @Test
    public void hdf5_960_euclidean() {
        String url = "http://ann-benchmarks.com/gist-960-euclidean.hdf5";

        DatasetReader reader = DatasetReader.create(url, workingDirectory, false);

        assertEquals(1_000_000, reader.getSize());
        assertEquals(960, reader.getDimension());
        assertEquals(960, reader.getTestDatasetDimension());

        assertNotNull(reader.getTrainVector(1234));
        assertEquals(1000, reader.getTestDataset().size());
    }
}
