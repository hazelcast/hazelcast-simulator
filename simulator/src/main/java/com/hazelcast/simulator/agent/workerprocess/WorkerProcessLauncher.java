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

    /// private final Agent agent;
    private final WorkerProcessManager processManager;
    private final WorkerProcessSettings settings;

    private File sessionDir;

    public WorkerProcessLauncher(WorkerProcessManager processManager,
                                 WorkerProcessSettings settings) {
        this.processManager = processManager;
        this.settings = settings;
    }

    public void launch() throws Exception {
        WorkerProcess process = null;
        try {
            sessionDir = processManager.getSessionDirectory();
            ensureExistingDirectory(sessionDir);

            WorkerType type = settings.getWorkerType();
            int workerIndex = settings.getWorkerIndex();
            LOGGER.info(format("Starting a Java Virtual Machine for %s Worker #%d", type, workerIndex));

            LOGGER.info("Launching Worker using settings: " + settings);
            process = startWorker();
            LOGGER.info(format("Finished starting a for %s Worker #%d", type, workerIndex));

            waitForWorkersStartup(process, settings.getWorkerStartupTimeout());
            process = null;
        } finally {
            if (process != null) {
                processManager.remove(process);
            }
        }
    }

    private WorkerProcess startWorker() throws IOException {
        int workerIndex = settings.getWorkerIndex();
        WorkerType type = settings.getWorkerType();

        SimulatorAddress agentAddress = processManager.getAgentAddress();

        SimulatorAddress workerAddress = new SimulatorAddress(
                AddressLevel.WORKER, agentAddress.getAgentIndex(), workerIndex, 0);
        String workerId = workerAddress.toString() + '-' + processManager.getPublicAddress() + '-' + type.name();
        File workerHome = ensureExistingDirectory(sessionDir, workerId);

        copyResourcesToWorkerHome(workerId);

        WorkerProcess workerProcess = new WorkerProcess(workerAddress, workerId, workerHome);

        generateWorkerStartScript(workerProcess);

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
                .directory(workerHome);

        Map<String, String> environment = processBuilder.environment();
        String javaHome = getJavaHome();
        String path = javaHome + "/bin:" + environment.get("PATH");
        environment.putAll(System.getenv());
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);
        environment.putAll(settings.getEnvironment());
        environment.put("CLASSPATH", getClasspath(workerHome));
        environment.put("SIMULATOR_HOME", getSimulatorHome().getAbsolutePath());
        environment.put("WORKER_ID", workerProcess.getId());
        environment.put("WORKER_TYPE", settings.getWorkerType().toString());
        environment.put("PUBLIC_ADDRESS", processManager.getPublicAddress());
        environment.put("WORKER_ADDRESS", workerAddress.toString());
        environment.put("AGENT_PORT", Integer.toString(processManager.getAgentPort()));

        Process process = processBuilder.start();

        workerProcess.setProcess(process);
        processManager.add(workerAddress, workerProcess);

        return workerProcess;
    }

    private void waitForWorkersStartup(WorkerProcess worker, int workerTimeoutSec) {
        int loopCount = (int) SECONDS.toMillis(workerTimeoutSec) / WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS;
        for (int i = 0; i < loopCount; i++) {
            if (hasExited(worker)) {
                throw new CreateWorkerFailedException(format(
                        "Startup of Worker %s on Agent %s failed, check log files in %s for more information!",
                        worker.getAddress(), processManager.getPublicAddress(), worker.getWorkerHome()));
            }

            String address = readAddress(worker);
            if (address != null) {
                worker.setHzAddress(address);
                LOGGER.info(format("Worker %s started", worker.getId()));
                return;
            }

            sleepMillis(WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS);
        }

        throw new CreateWorkerFailedException(format(
                "Worker %s on Agent %s didn't start within %s seconds, check log files in %s for more information!",
                worker.getAddress(), processManager.getPublicAddress(), workerTimeoutSec, worker.getWorkerHome()));
    }

    private String getJavaHome() {
        String javaHome = System.getProperty("java.home");
        String jre = "/jre";
        if (javaHome.endsWith(jre)) {
            javaHome = javaHome.substring(0, javaHome.length() - jre.length());
        }

        if (javaHomePrinted.compareAndSet(false, true)) {
            LOGGER.info("java.home=" + javaHome);
        }
        return javaHome;
    }

    private void generateWorkerStartScript(WorkerProcess workerProcess) {
        File startScript = new File(workerProcess.getWorkerHome(), "worker.sh");
        String script = settings.getWorkerScript();
        writeText(script, startScript);
    }

    private void copyResourcesToWorkerHome(String workerId) {
        File workersHome = new File(getSimulatorHome(), WORKERS_HOME_NAME);
        String sessionId = processManager.getSessionId();
        File uploadDirectory = new File(workersHome, sessionId + "/upload/").getAbsoluteFile();
        if (!uploadDirectory.exists() || !uploadDirectory.isDirectory()) {
            LOGGER.debug("Skip copying upload directory to workers since no upload directory was found");
            return;
        }
        String copyCommand = format("cp -rfv %s/%s/upload/* %s/%s/%s/ || true",
                workersHome,
                sessionId,
                workersHome,
                sessionId,
                workerId);
        execute(copyCommand);
        LOGGER.info(format("Finished copying '%s' to Worker", workersHome));
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
        String hzVersionDirectory = directoryForVersionSpec(settings.getVersionSpec());
        String testJarVersion = getHazelcastVersionFromJAR(simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*");
        LOGGER.info(format("Adding Hazelcast %s and test JARs %s to classpath", hzVersionDirectory, testJarVersion));

        String uploadClassPath = "";
        File uploadDirectory = new File(workerHome, "upload").getAbsoluteFile();
        if (uploadDirectory.exists() && uploadDirectory.isDirectory()) {
            File[] files = uploadDirectory.listFiles();
            if (files != null && files.length > 0) {
                uploadClassPath = CLASSPATH_SEPARATOR + uploadDirectory.getAbsolutePath() + "/*";
                LOGGER.info(format("Adding upload directory %s to classpath", uploadClassPath));
            }
        }

        // we have to reverse the classpath to monkey patch version specific classes
        return new File(processManager.getSessionDirectory(), "lib/*").getAbsolutePath()
                + CLASSPATH_SEPARATOR + workerHome.getAbsolutePath() + "/upload/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/user-lib/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/" + testJarVersion + "/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/test-lib/common/*"
                + CLASSPATH_SEPARATOR + simulatorHome + "/hz-lib/" + hzVersionDirectory + "/*"
                + uploadClassPath
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
