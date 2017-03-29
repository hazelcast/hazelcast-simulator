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

import com.hazelcast.simulator.common.ShutdownThread;
import com.hazelcast.simulator.protocol.Server;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.ExceptionReporter;
import com.hazelcast.simulator.vendors.VendorDriver;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;
import com.hazelcast.simulator.worker.performance.PerformanceMonitor;
import com.hazelcast.simulator.worker.testcontainer.TestManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.common.GitInfo.getBuildTime;
import static com.hazelcast.simulator.common.GitInfo.getCommitIdAbbrev;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.fillString;
import static com.hazelcast.simulator.utils.NativeUtils.getInputArgs;
import static com.hazelcast.simulator.utils.NativeUtils.getPID;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static com.hazelcast.simulator.vendors.VendorDriver.loadVendorDriver;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

public class Worker {

    private static final String DASHES = "---------------------------";
    private static final Logger LOGGER = Logger.getLogger(Worker.class);

    private final AtomicBoolean shutdownStarted = new AtomicBoolean();
    private final String publicAddress;
    private final PerformanceMonitor performanceMonitor;
    private final Server server;
    private final TestManager testManager;
    private final VendorDriver vendorDriver;
    private final Map<String, String> parameters;
    private final SimulatorAddress workerAddress;
    private ShutdownThread shutdownThread;

    public Worker(Map<String, String> parameters) throws Exception {
        this.parameters = parameters;
        this.publicAddress = parameters.get("PUBLIC_ADDRESS");
        this.workerAddress = SimulatorAddress.fromString(parameters.get("WORKER_ADDRESS"));

        this.vendorDriver = loadVendorDriver(parameters.get("VENDOR"))
                .setAll(parameters);

        this.server = new Server("workers")
                .setBrokerURL(localIp(), parseInt(parameters.get("AGENT_PORT")))
                .setSelfAddress(workerAddress);

        this.testManager = new TestManager(server, vendorDriver);

        ScriptExecutor scriptExecutor = new ScriptExecutor(vendorDriver);

        server.setProcessor(new WorkerOperationProcessor(this, testManager, scriptExecutor));

        Runtime.getRuntime().addShutdownHook(new WorkerShutdownThread(true));

        int interval = Integer.parseInt(parameters.get("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS"));
        this.performanceMonitor = new PerformanceMonitor(server, testManager, interval);
    }

    public void start() throws Exception {
        logInterestingJvmSettings();

        server.start();

        performanceMonitor.start();

        vendorDriver.createVendorInstance();

        // we need to signal start after everything has completed. Otherwise messages could be send on the agent topic
        // without the agent being subscribed.
        writeText("" + getPID(), new File(getUserDir(), "worker.pid"));

        logHeader("Successfully started Worker #" + workerAddress);
    }

    public void shutdown(TerminateWorkerOperation op) {
        LOGGER.warn("Terminating worker");
        closeQuietly(server);
        shutdownThread = new WorkerShutdownThread(op.isRealShutdown());
        shutdownThread.start();
    }

    // just for testing
    void awaitShutdown() throws Exception {
        if (shutdownThread != null) {
            shutdownThread.awaitShutdown();
        }
    }

    String getPublicIpAddress() {
        return publicAddress;
    }

    public static void main(String[] args) {
        try {
            log("Hazelcast Simulator Worker");
            log("Version: %s, Commit: %s, Build Time: %s", getSimulatorVersion(), getCommitIdAbbrev(), getBuildTime());
            log("SIMULATOR_HOME: %s%n", getSimulatorHome().getAbsolutePath());

            Map<String, String> properties = loadParameters();

            Worker worker = new Worker(properties);
            worker.start();
        } catch (Throwable e) {
            ExceptionReporter.report(null, e);
            exitWithError(LOGGER, "Failed to start Hazelcast Simulator Worker!", e);
        }
    }

    private static Map<String, String> loadParameters() throws IOException {
        Properties p = new Properties();
        p.load(new StringReader(fileAsText(new File(getUserDir(), "parameters"))));

        Map<String, String> properties = new HashMap<String, String>();
        for (Map.Entry<Object, Object> entry : p.entrySet()) {
            properties.put("" + entry.getKey(), "" + entry.getValue());
        }
        return properties;
    }

    private void logInterestingJvmSettings() {
        String type = parameters.get("WORKER_TYPE");
        logHeader("Hazelcast Worker #" + workerAddress + " (" + type + ')');
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
        log("jvm.args=%s", getInputArgs());
        log("process ID: " + getPID());
        log("Public address: " + publicAddress);
    }

    private static void logSystemProperty(String name) {
        log("%s=%s", name, System.getProperty(name));
    }

    private static void logHeader(String header) {
        StringBuilder builder = new StringBuilder();
        builder.append(DASHES).append(' ').append(header).append(' ').append(DASHES);

        String dashes = fillString(builder.length(), '-');
        log(dashes);
        log(builder.toString());
        log(dashes);
    }

    private static void log(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private final class WorkerShutdownThread extends ShutdownThread {

        private WorkerShutdownThread(boolean realShutdown) {
            super("WorkerShutdownThread", shutdownStarted, realShutdown);
        }

        @Override
        public void doRun() {
            closeQuietly(vendorDriver);
            closeQuietly(performanceMonitor);
        }
    }
}
