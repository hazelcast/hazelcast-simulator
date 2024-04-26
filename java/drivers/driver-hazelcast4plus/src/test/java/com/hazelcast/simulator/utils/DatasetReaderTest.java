package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.tests.vector.DatasetReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class DatasetReaderTest {

    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/random_keywords_1m.tgz";
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/hnm.tgz"; // work
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/yandex_t2i_gt_100k.tgz"; // not vectors in archive
    //String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/laion-small-clip.tgz"; // broken?

    String workingDirectory = "/Users/oshultseva/Downloads/dataset_output/";
    @Test
    public void npyArchive() {
        String url = "https://storage.googleapis.com/ann-filtered-benchmark/datasets/dbpedia_openai_1M.tgz";

        DatasetReader reader = DatasetReader.create(url, workingDirectory);

        assertEquals(975_000, reader.getSize());
        assertEquals(1536, reader.getDimension());
        assertEquals(1536, reader.getTestDatasetDimension());

        assertNotNull(reader.getTrainVector(1234));
        assertNotNull(reader.getTestDataset());
    }

    @Test
    public void hdf5() {
        String url = "http://ann-benchmarks.com/gist-960-euclidean.hdf5";

        DatasetReader reader = DatasetReader.create(url, workingDirectory);

        assertEquals(1_000_000, reader.getSize());
        assertEquals(960, reader.getDimension());
        assertEquals(960, reader.getTestDatasetDimension());

        assertNotNull(reader.getTrainVector(1234));
        assertNotNull(reader.getTestDataset());
    }
}
