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
package com.hazelcast.simulator.agent.workerprocess;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.BuildInfoUtils.getHazelcastVersionFromJAR;
import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Responsible for launching {@link WorkerProcess} instances.
 */
public class WorkerProcessLauncher {

    public static final String WORKERS_HOME_NAME = "workers";

    private static final int WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS = 500;

    private static final String CLASSPATH = System.getProperty("java.class.path");
    private static final String CLASSPATH_SEPARATOR = System.getProperty("path.separator");

    private static final Logger LOGGER = Logger.getLogger(WorkerProcessLauncher.class);

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

    private final Agent agent;
    private final WorkerProcessManager workerProcessManager;
    private final WorkerProcessSettings workerProcessSettings;
    private File sessionDir;

    public WorkerProcessLauncher(Agent agent,
                                 WorkerProcessManager workerProcessManager,
                                 WorkerProcessSettings workerProcessSettings) {
        this.agent = agent;
        this.workerProcessManager = workerProcessManager;
        this.workerProcessSettings = workerProcessSettings;
    }

    public void launch() {
        try {
            sessionDir = agent.getSessionDirectory();
            ensureExistingDirectory(sessionDir);

            WorkerType type = workerProcessSettings.getWorkerType();
            int workerIndex = workerProcessSettings.getWorkerIndex();
            LOGGER.info(format("Starting a Java Virtual Machine for %s Worker #%d", type, workerIndex));

            LOGGER.info("Spawning Worker using settings: " + workerProcessSettings);
            WorkerProcess worker = startWorker();
            LOGGER.info(format("Finished starting a for %s Worker #%d", type, workerIndex));

            waitForWorkersStartup(worker, workerProcessSettings.getWorkerStartupTimeout());
        } catch (Exception e) {
            throw new SpawnWorkerFailedException("Failed to start Worker", e);
        }
    }

    private WorkerProcess startWorker() throws IOException {
        int workerIndex = workerProcessSettings.getWorkerIndex();
        WorkerType type = workerProcessSettings.getWorkerType();

        SimulatorAddress workerAddress = new SimulatorAddress(
                AddressLevel.WORKER, agent.getAddressIndex(), workerIndex, 0);
        String workerId = workerAddress.toString() + '-' + agent.getPublicAddress() + '-' + type.name();
        File workerHome = ensureExistingDirectory(sessionDir, workerId);

        copyResourcesToWorkerHome(workerId);

        WorkerProcess workerProcess = new WorkerProcess(workerAddress, workerId, workerHome);

        generateWorkerStartScript(workerProcess);

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
                .directory(workerHome);

        Map<String, String> environment = processBuilder.environment();
        String javaHome = getJavaHome();
        String path = javaHome + "/bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);
        environment.putAll(workerProcessSettings.getEnvironment());
        environment.put("CLASSPATH", getClasspath(workerHome));
        environment.put("SIMULATOR_HOME", getSimulatorHome().getAbsolutePath());
        environment.put("WORKER_ID", workerProcess.getId());
        environment.put("WORKER_TYPE", workerProcessSettings.getWorkerType().toString());
        environment.put("PUBLIC_ADDRESS", agent.getPublicAddress());
        environment.put("AGENT_INDEX", Integer.toString(agent.getAddressIndex()));
        environment.put("WORKER_INDEX", Integer.toString(workerProcessSettings.getWorkerIndex()));
        environment.put("WORKER_PORT", Integer.toString(agent.getPort() + workerProcessSettings.getWorkerIndex()));

        Process process = processBuilder.start();

        workerProcess.setProcess(process);
        workerProcessManager.add(workerAddress, workerProcess);

        return workerProcess;
    }

    private void waitForWorkersStartup(WorkerProcess worker, int workerTimeoutSec) {
        int loopCount = (int) SECONDS.toMillis(workerTimeoutSec) / WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS;
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

    private void generateWorkerStartScript(WorkerProcess workerProcess) {
        File startScript = new File(workerProcess.getWorkerHome(), "worker.sh");
        String script = workerProcessSettings.getWorkerScript();
        writeText(script, startScript);
    }

    private void copyResourcesToWorkerHome(String workerId) {
        LOGGER.warn("------------------------");

        File workerHome = new File(getSimulatorHome(), WORKERS_HOME_NAME);
        String sessionId = agent.getSessionId();
        File uploadDirectory = new File(workerHome, sessionId + "/upload/").getAbsoluteFile();
        if (!uploadDirectory.exists() || !uploadDirectory.isDirectory()) {
            LOGGER.warn("Skip copying upload directory to workers since no upload directory was found");
            return;
        }

        String copyCommand = format("cp -rfv %s/%s/upload/. %s/%s/%s/upload",
                workerHome,
                sessionId,
                workerHome,
                sessionId,
                workerId);
        execute(copyCommand);

        LOGGER.warn(copyCommand);

        LOGGER.info(format("Finished copying '%s' to Worker", workerHome));
    }

    private boolean hasExited(WorkerProcess workerProcess) {
        try {
            workerProcess.getProcess().exitValue();
            return true;
        } catch (IllegalThreadStateException e) {
            return false;
        }
    }

    private String readAddress(WorkerProcess workerProcess) {
        File file = new File(workerProcess.getWorkerHome(), "worker.address");
        if (!file.exists()) {
            return null;
        }

        String address = fileAsText(file);
        deleteQuiet(file);

        return address;
    }

    private String getClasspath(File workerHome) {
        String simulatorHome = getSimulatorHome().getAbsolutePath();
        String hzVersionDirectory = directoryForVersionSpec(workerProcessSettings.getVersionSpec());
        String testJarVersion = getHazelcastVersionFromJAR(simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*");
        LOGGER.info(format("Adding Hazelcast %s and test JARs %s to classpath", hzVersionDirectory, testJarVersion));

        // we have to reverse the classpath to monkey patch version specific classes
        return new File(agent.getSessionDirectory(), "lib/*").getAbsolutePath()
                + CLASSPATH_SEPARATOR + workerHome.getAbsolutePath() + "/upload/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/user-lib/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/" + testJarVersion + "/*"
                + CLASSPATH_SEPARATOR + "upload/lib/"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/common/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*"
                + CLASSPATH_SEPARATOR + CLASSPATH;
    }

    private static String directoryForVersionSpec(String versionSpec) {
        if ("bringmyown".equals(versionSpec)) {
            return null;
        }
        if ("outofthebox".equals(versionSpec)) {
            return "outofthebox";
        }
        String s = versionSpec.replace('=', '-');

        // we need to replace all forward slashes by double back slashes.
        StringBuilder result = new StringBuilder();
        for (char c : s.toCharArray()) {
            if (c == '/') {
                result.append('\\').append('\\');
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }
}
