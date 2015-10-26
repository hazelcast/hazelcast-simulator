package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.protocol.connector.ServerConnector;
import com.hazelcast.simulator.utils.NativeUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.FileUtils.writeObject;
import static com.hazelcast.simulator.utils.FileUtils.writeText;

public class IntegrationTestWorker implements Worker {

    private static final Logger LOGGER = Logger.getLogger(IntegrationTestWorker.class);

    private final CountDownLatch latch = new CountDownLatch(1);

    public IntegrationTestWorker() throws Exception {
        LOGGER.info("Starting IntegrationTestWorker...");

        Runtime.getRuntime().addShutdownHook(new ShutdownThread(latch));

        int pid = NativeUtils.getPID();
        LOGGER.info("PID: " + pid);
        File pidFile = new File("worker.pid");
        writeText("" + pid, pidFile);

        File addressFile = new File("worker.address");
        writeObject("127.0.0.1:5701", addressFile);

        LOGGER.info("Waiting for shutdown...");
        latch.await(5, TimeUnit.SECONDS);

        LOGGER.info("Done!");
    }

    @Override
    public void shutdown() {
        new ShutdownThread(latch).start();
    }

    @Override
    public boolean startPerformanceMonitor() {
        return true;
    }

    @Override
    public void shutdownPerformanceMonitor() {
    }

    @Override
    public ServerConnector getServerConnector() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        new IntegrationTestWorker();
    }

    private final class ShutdownThread extends Thread {

        private final CountDownLatch latch;

        public ShutdownThread(CountDownLatch latch) {
            super("WorkerShutdownThread");
            setDaemon(true);

            this.latch = latch;
        }

        @Override
        public void run() {
            LOGGER.info("Stopping IntegrationTestWorker...");
            latch.countDown();

            // makes sure that log4j will always flush the log buffers
            LOGGER.info("Stopping log4j...");
            LogManager.shutdown();
        }
    }
}
