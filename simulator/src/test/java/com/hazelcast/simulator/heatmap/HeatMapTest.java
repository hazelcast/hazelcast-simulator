package com.hazelcast.simulator.heatmap;

import com.hazelcast.simulator.provisioner.AbstractComputeServiceTest;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.net.URL;

import static com.hazelcast.simulator.heatmap.HeatMap.createHistogramLogReader;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HeatMapTest extends AbstractComputeServiceTest {

    private File directory;

    private HeatMap heatMap;

    @Before
    public void setUp() {
        directory = ensureExistingDirectory("workers");

        File directory1 = ensureExistingDirectory(directory, "workers1");
        File directory2 = ensureExistingDirectory(directory, "workers2");

        ensureExistingFile(directory1, "latency-HeatMapTest-aggregated.txt");
        ensureExistingFile(directory2, "latency-HeatMapTest-aggregated.txt");

        ensureExistingFile(directory1, "latency-IntIntMapTest-aggregated.txt");
        ensureExistingFile(directory2, "latency-IntIntMapTest-aggregated.txt");

        ClassLoader classLoader = getClass().getClassLoader();
        createLatencyFile(classLoader, "heatmap-test-sample-1.txt", directory1);
        createLatencyFile(classLoader, "heatmap-test-sample-2.txt", directory2);

        heatMap = new HeatMap(directory.getAbsolutePath(), "HeatMapTest", "");
    }

    @After
    public void tearDown() {
        deleteQuiet(directory);
    }

    @Test
    public void testCreateHeatMap() {
        heatMap.createHeatMap();

        assertEquals(6, heatMap.getHistogramCount());
    }

    @Test
    public void testCreateHeatMap_invalidDirectory() {
        heatMap = new HeatMap(new File("/dev/null").getAbsolutePath(), "", "");
        heatMap.createHeatMap();

        assertEquals(0, heatMap.getHistogramCount());
    }

    @Test(expected = CommandLineExitException.class)
    public void testCreateHistogramLogReader_invalidFile() {
        createHistogramLogReader(new File("notFound"), "HeatMapTest");
    }

    private static void createLatencyFile(ClassLoader classLoader, String resourceFile, File directory) {
        URL resource1 = classLoader.getResource(resourceFile);
        assertNotNull(resource1);

        File latencyFile = new File(directory, "latency-HeatMapTest-HeatMapTestWorkerProbe.txt");
        appendText(fileAsText(new File(resource1.getFile())), latencyFile);
    }
}
