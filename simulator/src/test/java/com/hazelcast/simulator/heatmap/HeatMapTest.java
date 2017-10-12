/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import static com.hazelcast.simulator.utils.TestUtils.createTmpDirectory;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class HeatMapTest extends AbstractComputeServiceTest {

    private File tmpDir;
    private File workersDirectory;
    private HeatMap heatMap;

    @Before
    public void before() {
        tmpDir = createTmpDirectory();
        workersDirectory = ensureExistingDirectory(tmpDir, "workers");

        File directory1 = ensureExistingDirectory(workersDirectory, "workers1");
        File directory2 = ensureExistingDirectory(workersDirectory, "workers2");

        ensureExistingFile(directory1, "latency-HeatMapTest-aggregated.txt");
        ensureExistingFile(directory2, "latency-HeatMapTest-aggregated.txt");

        ensureExistingFile(directory1, "latency-IntIntMapTest-aggregated.txt");
        ensureExistingFile(directory2, "latency-IntIntMapTest-aggregated.txt");

        ClassLoader classLoader = getClass().getClassLoader();
        createLatencyFile(classLoader, "heatmap-test-sample-1.txt", directory1);
        createLatencyFile(classLoader, "heatmap-test-sample-2.txt", directory2);

        heatMap = new HeatMap(workersDirectory.getAbsolutePath(), "HeatMapTest", "");
    }

    @After
    public void tearDown() {
        deleteQuiet(tmpDir);
        deleteQuiet(workersDirectory);
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
