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
package com.hazelcast.simulator.agent.workerjvm;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.agent.SpawnWorkerFailedException;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.BuildInfoUtils.getHazelcastVersionFromJAR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.directoryForVersionSpec;
import static java.lang.String.format;

public class WorkerJvmLauncher {

    public static final String WORKERS_HOME_NAME = "workers";

    private static final int WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS = 500;

    private static final String CLASSPATH = System.getProperty("java.class.path");
    private static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");

    private static final Logger LOGGER = Logger.getLogger(WorkerJvmLauncher.class);

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

    private final Agent agent;
    private final WorkerJvmManager workerJvmManager;
    private final WorkerJvmSettings workerJvmSettings;

    private File hzConfigFile;
    private File log4jFile;
    private File testSuiteDir;

    public WorkerJvmLauncher(Agent agent, WorkerJvmManager workerJvmManager, WorkerJvmSettings workerJvmSettings) {
        this.agent = agent;
        this.workerJvmManager = workerJvmManager;
        this.workerJvmSettings = workerJvmSettings;
    }

    public void launch() {
        try {
            testSuiteDir = agent.getTestSuiteDir();
            ensureExistingDirectory(testSuiteDir);

            WorkerType type = workerJvmSettings.getWorkerType();
            int workerIndex = workerJvmSettings.getWorkerIndex();
            LOGGER.info(format("Starting a Java Virtual Machine for %s Worker #%d", type, workerIndex));

            LOGGER.info("Spawning Worker JVM using settings: " + workerJvmSettings);
            WorkerJvm worker = startWorkerJvm();
            LOGGER.info(format("Finished starting a JVM for %s Worker #%d", type, workerIndex));

            waitForWorkersStartup(worker, workerJvmSettings.getWorkerStartupTimeout());
        } catch (Exception e) {
            throw new SpawnWorkerFailedException("Failed to start Worker", e);
        }
    }

    private WorkerJvm startWorkerJvm() throws IOException {
        int workerIndex = workerJvmSettings.getWorkerIndex();
        WorkerType type = workerJvmSettings.getWorkerType();

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, agent.getAddressIndex(), workerIndex, 0);
        String workerId = "worker-" + workerAddress + '-' + agent.getPublicAddress() + '-' + type.toLowerCase();
        File workerHome = ensureExistingDirectory(testSuiteDir, workerId);

        String hzConfigFileName = (type == WorkerType.MEMBER) ? "hazelcast" : "client-hazelcast";
        hzConfigFile = ensureExistingFile(workerHome, hzConfigFileName + ".xml");
        writeText(workerJvmSettings.getHazelcastConfig(), hzConfigFile);

        log4jFile = ensureExistingFile(workerHome, "log4j.xml");
        writeText(workerJvmSettings.getLog4jConfig(), log4jFile);

        WorkerJvm workerJvm = new WorkerJvm(workerAddress, workerId, workerHome);

        generateWorkerStartScript(workerJvm);

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
                .directory(workerHome);

        Map<String, String> environment = processBuilder.environment();
        String javaHome = getJavaHome();
        String path = javaHome + "/bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();

        workerJvm.setProcess(process);
        copyResourcesToWorkerId(workerId);
        workerJvmManager.add(workerAddress, workerJvm);

        return workerJvm;
    }

    private void waitForWorkersStartup(WorkerJvm worker, int workerTimeoutSec) {
        int loopCount = (int) TimeUnit.SECONDS.toMillis(workerTimeoutSec) / WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS;
        for (int i = 0; i < loopCount; i++) {
            if (hasExited(worker)) {
                throw new SpawnWorkerFailedException(format(
                        "Startup of Worker %s on Agent %s failed, check log files in %s for more information!",
                        worker.getAddress(), agent.getPublicAddress(), worker.getWorkerHome()));
            }

            String address = readAddress(worker);
            if (address != null) {
                worker.setHzAddress(address);
                LOGGER.info(format("Worker %s started", worker.getId()));
                return;
            }

            sleepMillis(WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS);
        }

        throw new SpawnWorkerFailedException(format(
                "Worker %s on Agent %s didn't start within %s seconds, check log files in %s for more information!",
                worker.getAddress(), agent.getPublicAddress(), workerTimeoutSec, worker.getWorkerHome()));
    }

    private String getJavaHome() {
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            LOGGER.info("java.home=" + javaHome);
        }
        return javaHome;
    }

    private void generateWorkerStartScript(WorkerJvm workerJvm) {
        File startScript = new File(workerJvm.getWorkerHome(), "worker.sh");

        String script = workerJvmSettings.getWorkerScript();
        script = replaceAll(script, "CLASSPATH", getClasspath());
        script = replaceAll(script, "JVM_OPTIONS", workerJvmSettings.getJvmOptions());
        script = replaceAll(script, "LOG4J_FILE", log4jFile.getAbsolutePath());
        script = replaceAll(script, "SIMULATOR_HOME", getSimulatorHome());
        script = replaceAll(script, "WORKER_ID", workerJvm.getId());
        script = replaceAll(script, "WORKER_TYPE", workerJvmSettings.getWorkerType());
        script = replaceAll(script, "PUBLIC_ADDRESS", agent.getPublicAddress());
        script = replaceAll(script, "AGENT_INDEX", agent.getAddressIndex());
        script = replaceAll(script, "WORKER_INDEX", workerJvmSettings.getWorkerIndex());
        script = replaceAll(script, "WORKER_PORT", agent.getPort() + workerJvmSettings.getWorkerIndex());
        script = replaceAll(script, "AUTO_CREATE_HZ_INSTANCE", workerJvmSettings.isAutoCreateHzInstance());
        script = replaceAll(script, "WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS",
                workerJvmSettings.getPerformanceMonitorIntervalSeconds());
        script = replaceAll(script, "HZ_CONFIG_FILE", hzConfigFile.getAbsolutePath());

        writeText(script, startScript);
    }

    private String replaceAll(String script, String variable, Object value) {
        return script.replaceAll("@" + variable, "" + value);
    }

    private void copyResourcesToWorkerId(String workerId) {
        File workersDir = new File(getSimulatorHome(), WORKERS_HOME_NAME);
        String testSuiteId = agent.getTestSuite().getId();
        File uploadDirectory = new File(workersDir, testSuiteId + "/upload/").getAbsoluteFile();
        if (!uploadDirectory.exists() || !uploadDirectory.isDirectory()) {
            LOGGER.debug("Skip copying upload directory to workers since no upload directory was found");
            return;
        }
        String copyCommand = format("cp -rfv %s/%s/upload/* %s/%s/%s/",
                workersDir,
                testSuiteId,
                workersDir,
                testSuiteId,
                workerId);
        execute(copyCommand);
        LOGGER.info(format("Finished copying '%s' to Worker", workersDir));
    }

    private boolean hasExited(WorkerJvm workerJvm) {
        try {
            workerJvm.getProcess().exitValue();
            return true;
        } catch (IllegalThreadStateException e) {
            return false;
        }
    }

    private String readAddress(WorkerJvm jvm) {
        File file = new File(jvm.getWorkerHome(), "worker.address");
        if (!file.exists()) {
            return null;
        }

        String address = fileAsText(file);
        deleteQuiet(file);

        return address;
    }

    private String getClasspath() {
        String simulatorHome = getSimulatorHome().getAbsolutePath();
        String hzVersionDirectory = directoryForVersionSpec(workerJvmSettings.getHazelcastVersionSpec());
        String testJarVersion = getHazelcastVersionFromJAR(simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*");
        LOGGER.info(format("Adding Hazelcast %s and test JARs %s to classpath", hzVersionDirectory, testJarVersion));

        // we have to reverse the classpath to monkey patch version specific classes
        return new File(agent.getTestSuiteDir(), "lib/*").getAbsolutePath()
                + CLASSPATH_SEPARATOR + simulatorHome + "/user-lib/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/" + testJarVersion + "/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/common/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*"
                + CLASSPATH_SEPARATOR + CLASSPATH;
    }
}
