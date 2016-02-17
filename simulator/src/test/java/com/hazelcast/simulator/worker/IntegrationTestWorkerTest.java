package com.hazelcast.simulator.worker;

import org.junit.After;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class IntegrationTestWorkerTest {

    private File workerAddressFile = new File("worker.address");
    private File workerPidFile = new File("worker.pid");

    private IntegrationTestWorker worker = new IntegrationTestWorker();

    @After
    public void tearDown() throws Exception {
        try {
            worker.shutdown(false);
            worker.awaitShutdown(5);
        } finally {
            deleteQuiet(workerAddressFile);
            deleteQuiet(workerPidFile);
        }
    }

    @Test
    public void testConstructor() {
        assertTrue(workerAddressFile.exists());
        assertTrue(workerPidFile.exists());
    }

    @Test
    public void testAwaitShutdown_withTimeout() throws Exception {
        worker.awaitShutdown(0);
    }

    @Test
    public void testStartPerformanceMonitor() {
        assertTrue(worker.startPerformanceMonitor());
    }

    @Test
    public void testShutdownPerformanceMonitor() {
        worker.shutdownPerformanceMonitor();
    }

    @Test
    public void testGetWorkerConnector() {
        assertNull(worker.getWorkerConnector());
    }

    @Test
    public void testGetPublicIpAddress() {
        assertNull(worker.getPublicIpAddress());
    }
}
