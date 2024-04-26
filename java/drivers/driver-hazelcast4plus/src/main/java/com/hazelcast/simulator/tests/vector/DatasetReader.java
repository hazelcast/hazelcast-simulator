package com.hazelcast.simulator.tests.vector;

import com.hazelcast.simulator.tests.vector.readers.HDF5DatasetReader;
import com.hazelcast.simulator.tests.vector.readers.NpyArchiveDatasetReader;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.LaxRedirectStrategy;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.nio.file.Path;

public abstract class DatasetReader {

    private final URL datasetURL;

    protected final Path workingDirectory;

    protected final File downloadedFile;

    protected float[][] trainDataset;

    protected float[][] testDataset;

    protected int dimension;

    protected int size;

    protected final Logger logger = LogManager.getLogger(getClass());

    public DatasetReader(String url, String directory) {
        try {
            this.datasetURL = URI.create(url).toURL();
            this.workingDirectory = Path.of(directory, FilenameUtils.getBaseName(datasetURL.getFile()));
            this.downloadedFile = Path.of(workingDirectory.toString(), FilenameUtils.getName(datasetURL.getFile())).toFile();

            logger.info("Start downloading file from " + datasetURL);
            if (!downloadedFile.exists()) {
                download();
            }
            logger.info("File downloaded to " + downloadedFile + ". Start unpacking...");

            preprocessDatasetFile();
            parseTrainDataset();
            parseTestDataset();
            logger.info("Dataset reader is initialized");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected abstract void preprocessDatasetFile();
    protected abstract void parseTrainDataset();

    protected abstract void parseTestDataset();

    private void cleanup() {
        try {
            FileUtils.cleanDirectory(workingDirectory.toFile());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public float[] getTrainVector(int index) {
        return trainDataset[index];
    }

    public float[][] getTestDataset() {
        return testDataset;
    }

    public int getDimension() {
        return dimension;
    }

    public int getSize() {
        return size;
    }

    public int getTestDatasetDimension() {
        if(testDataset.length == 0) {
            return 0;
        }
        return testDataset[0].length;
    }

    private void download() {
        CloseableHttpClient httpClient = HttpClients.custom()
                .setRedirectStrategy(new LaxRedirectStrategy())
                .build();
        try {
            HttpGet get = new HttpGet(datasetURL.toURI());
            httpClient.execute(get, new FileDownloadResponseHandler(downloadedFile));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        } finally {
            IOUtils.closeQuietly(httpClient);
        }
    }


    private static class FileDownloadResponseHandler implements ResponseHandler<Void> {

        private final File target;

        public FileDownloadResponseHandler(File target) {
            this.target = target;
        }

        @Override
        public Void handleResponse(HttpResponse response) throws IOException {
            InputStream source = response.getEntity().getContent();
            FileUtils.copyInputStreamToFile(source, this.target);
            return null;
        }
    }

    public static DatasetReader create(String url, String directory) {
        try {
            URL datasetUrl = URI.create(url).toURL();
            var ext = FilenameUtils.getExtension(datasetUrl.getFile());
            return switch (ext) {
                case "hdf5" -> new HDF5DatasetReader(url, directory);
                case "tgz" -> new NpyArchiveDatasetReader(url, directory);
                default -> throw new UnsupportedOperationException("File " + ext + " is not supported");
            };
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }

    }
}
