/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.operation.OperationTypeCounter;
import com.hazelcast.simulator.protocol.processors.WorkerOperationProcessor;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.worker.performance.WorkerPerformanceMonitor;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.HazelcastUtils.createClientHazelcastInstance;
import static com.hazelcast.simulator.utils.HazelcastUtils.createServerHazelcastInstance;
import static com.hazelcast.simulator.utils.HazelcastUtils.getHazelcastAddress;
import static com.hazelcast.simulator.utils.HazelcastUtils.warmupPartitions;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public final class MemberWorker implements Worker {

    private static final String DASHES = "---------------------------";

    private static final Logger LOGGER = Logger.getLogger(MemberWorker.class);
    private static final AtomicBoolean SHUTDOWN_STARTED = new AtomicBoolean();

    private final WorkerType type;
    private final String publicAddress;

    private final boolean autoCreateHzInstance;
    private final String hzConfigFile;

    private final HazelcastInstance hazelcastInstance;
    private final WorkerConnector workerConnector;

    private final WorkerPerformanceMonitor workerPerformanceMonitor;

    private ShutdownThread shutdownThread;

    MemberWorker(WorkerType type, String publicAddress, int agentIndex, int workerIndex, int workerPort, String hzConfigFile,
                 boolean autoCreateHzInstance, int workerPerformanceMonitorIntervalSeconds) throws Exception {
        SHUTDOWN_STARTED.set(false);
        this.type = type;
        this.publicAddress = publicAddress;

        this.autoCreateHzInstance = autoCreateHzInstance;
        this.hzConfigFile = hzConfigFile;

        this.hazelcastInstance = getHazelcastInstance();
        this.workerConnector = WorkerConnector.createInstance(agentIndex, workerIndex, workerPort, type, hazelcastInstance, this);

        this.workerPerformanceMonitor = initWorkerPerformanceMonitor(workerPerformanceMonitorIntervalSeconds);

        Runtime.getRuntime().addShutdownHook(new WorkerShutdownThread(true));

        signalStartToAgent();
    }

    void start() {
        workerConnector.start();

        if (workerPerformanceMonitor != null) {
            workerPerformanceMonitor.start();
        }
    }

    @Override
    public void shutdown(boolean shutdownLog4j) {
        shutdownThread = new WorkerShutdownThread(shutdownLog4j);
        shutdownThread.start();
    }

    // just for testing
    void awaitShutdown() throws Exception {
        if (shutdownThread != null) {
            shutdownThread.awaitShutdown();
        }
    }

    @Override
    public WorkerConnector getWorkerConnector() {
        return workerConnector;
    }

    @Override
    public String getPublicIpAddress() {
        return publicAddress;
    }

    private HazelcastInstance getHazelcastInstance() throws Exception {
        HazelcastInstance instance = null;
        if (autoCreateHzInstance) {
            logHeader("Creating " + type + " HazelcastInstance");
            switch (type) {
                case CLIENT:
                    instance = createClientHazelcastInstance(hzConfigFile);
                    break;
                default:
                    instance = createServerHazelcastInstance(hzConfigFile);
            }
            logHeader("Successfully created " + type + " HazelcastInstance");

            warmupPartitions(instance);
        }
        return instance;
    }

    private WorkerPerformanceMonitor initWorkerPerformanceMonitor(int intervalSeconds) {
        if (intervalSeconds < 1) {
            return null;
        }
        WorkerOperationProcessor processor = (WorkerOperationProcessor) workerConnector.getProcessor();
        return new WorkerPerformanceMonitor(workerConnector, processor.getTests(), 1, TimeUnit.SECONDS);
    }

    private void signalStartToAgent() {
        String address = getHazelcastAddress(type, publicAddress, hazelcastInstance);
        File file = new File("worker.address");
        writeText(address, file);
    }

    public static void main(String[] args) {
        try {
            startWorker();
        } catch (Exception e) {
            ExceptionReporter.report(null, e);
            exitWithError(LOGGER, "Could not start Hazelcast Simulator Worker!", e);
        }
    }

    static MemberWorker startWorker() throws Exception {
        echo("Hazelcast Simulator Worker");
        echo("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
        echo("SIMULATOR_HOME: %s%n", getSimulatorHome().getAbsolutePath());

        String workerId = System.getProperty("workerId");
        WorkerType type = WorkerType.valueOf(System.getProperty("workerType"));

        String publicAddress = System.getProperty("publicAddress");
        int agentIndex = parseInt(System.getProperty("agentIndex"));
        int workerIndex = parseInt(System.getProperty("workerIndex"));
        int workerPort = parseInt(System.getProperty("workerPort"));
        String hzConfigFile = System.getProperty("hzConfigFile");

        boolean autoCreateHzInstance = parseBoolean(System.getProperty("autoCreateHzInstance", "true"));
        int workerPerformanceMonitorIntervalSeconds = parseInt(System.getProperty("workerPerformanceMonitorIntervalSeconds"));

        logHeader("Hazelcast Worker #" + workerIndex + " (" + type + ')');
        logInputArguments();
        logInterestingSystemProperties();
        echo("process ID: " + getPID());

        echo("Worker id: " + workerId);
        echo("Worker type: " + type);

        echo("Public address: " + publicAddress);
        echo("Agent index: " + agentIndex);
        echo("Worker index: " + workerIndex);
        echo("Worker port: " + workerPort);

        echo("autoCreateHzInstance: " + autoCreateHzInstance);
        echo("workerPerformanceMonitorIntervalSeconds: " + workerPerformanceMonitorIntervalSeconds);

        MemberWorker worker = new MemberWorker(type, publicAddress, agentIndex, workerIndex, workerPort, hzConfigFile,
                autoCreateHzInstance, workerPerformanceMonitorIntervalSeconds);
        worker.start();

        logHeader("Successfully started Hazelcast Worker #" + workerIndex);

        return worker;
    }

    private static void logInputArguments() {
        List<String> inputArguments = ManagementFactory.getRuntimeMXBean().getInputArguments();
        echo("JVM input arguments: " + inputArguments);
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("sun.java.command");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("SIMULATOR_HOME");
        logSystemProperty("hazelcast.logging.type");
    }

    private static void logSystemProperty(String name) {
        echo("%s=%s", name, System.getProperty(name));
    }

    private static void logHeader(String header) {
        StringBuilder builder = new StringBuilder();
        builder.append(DASHES).append(' ').append(header).append(' ').append(DASHES);

        String dashes = fillString(builder.length(), '-');
        echo(dashes);
        echo(builder.toString());
        echo(dashes);
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private final class WorkerShutdownThread extends ShutdownThread {

        private WorkerShutdownThread(boolean shutdownLog4j) {
            super("WorkerShutdownThread", SHUTDOWN_STARTED, shutdownLog4j);
        }

        @Override
        public void doRun() {
            if (hazelcastInstance != null) {
                echo("Stopping HazelcastInstance...");
                hazelcastInstance.shutdown();
            }

            if (workerPerformanceMonitor != null) {
                echo("Shutting down WorkerPerformanceMonitor");
                try {
                    workerPerformanceMonitor.shutdown();
                } catch (InterruptedException e) {
                    echo("Failed wait for WorkerPerformanceMonitor shutdown ");
                }
            }

            if (workerConnector != null) {
                echo("Stopping WorkerConnector...");
                workerConnector.shutdown();
            }

            OperationTypeCounter.printStatistics(Level.INFO);
        }
    }
}
