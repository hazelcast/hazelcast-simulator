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

import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.ensureFreshDirectory;
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
    private static final String FILE_PREFIX = "file:";

    private final AtomicBoolean javaHomePrinted = new AtomicBoolean();

    /// private final Agent agent;
    private final WorkerProcessManager processManager;
    private final WorkerParameters parameters;
    private final SimulatorAddress workerAddress;

    private File sessionDir;

    WorkerProcessLauncher(WorkerProcessManager processManager,
                          WorkerParameters parameters) {
        this.processManager = processManager;
        this.parameters = parameters;
        this.workerAddress = SimulatorAddress.fromString(parameters.get("WORKER_ADDRESS"));
    }

    void launch() throws Exception {
        WorkerProcess process = null;
        try {
            sessionDir = getSessionDirectory();
            ensureExistingDirectory(sessionDir);

            String type = parameters.getWorkerType();
            LOGGER.info(format("Starting a Java Virtual Machine for %s Worker %s", type, workerAddress));

            LOGGER.info("Launching Worker using: " + parameters);
            process = startWorker();
            LOGGER.info(format("Finished starting a for %s Worker %s ", type, workerAddress));

            waitForWorkersStartup(process);
            process = null;
        } finally {
            if (process != null) {
                processManager.remove(process);
            }
        }
    }

    private File getSessionDirectory() {
        String sessionId = parameters.get("SESSION_ID");
        File workersDir = ensureExistingDirectory(getSimulatorHome(), "workers");
        return ensureExistingDirectory(workersDir, sessionId);
    }

    private WorkerProcess startWorker() throws IOException {
        String workerDirName = parameters.get("WORKER_DIR_NAME");
        File workerHome = ensureFreshDirectory(new File(sessionDir, workerDirName));

        copyResourcesToWorkerHome(workerDirName);

        WorkerProcess workerProcess = new WorkerProcess(workerAddress, workerDirName, workerHome);

        ProcessBuilder processBuilder = new ProcessBuilder("bash", "worker.sh")
                .directory(workerHome);

        Map<String, String> environment = processBuilder.environment();

        StringBuilder sb = new StringBuilder();
        List<String> keys = new ArrayList<>(parameters.asMap().keySet());
        Collections.sort(keys);
        for (String key : keys) {
            String value = parameters.get(key);
            if (key.startsWith(FILE_PREFIX)) {
                String fileName = key.substring(FILE_PREFIX.length(), key.length());
                writeText(value, new File(workerHome, fileName));
            } else {
                environment.put(key, value);
                sb.append(key).append("=").append(value).append("\n");
            }
        }
        sb.append("CLASSPATH=").append(getClasspath(workerHome)).append("\n");

        writeText(sb.toString(), new File(workerHome, "parameters"));

        environment.putAll(System.getenv());
        String javaHome = getJavaHome();
        String path = javaHome + "/bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);
        environment.put("CLASSPATH", getClasspath(workerHome));
        environment.put("SIMULATOR_HOME", getSimulatorHome().getAbsolutePath());

        Process process = processBuilder.start();

        workerProcess.setProcess(process);
        processManager.add(workerAddress, workerProcess);

        return workerProcess;
    }

    private void waitForWorkersStartup(WorkerProcess worker) {
        int timeout = parameters.intGet("WORKER_STARTUP_TIMEOUT_SECONDS");

        int loopCount = (int) SECONDS.toMillis(timeout) / WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS;
        for (int i = 0; i < loopCount; i++) {
            if (hasExited(worker)) {
                throw new CreateWorkerFailedException(format(
                        "Startup of Worker %s on Agent %s failed, check log files in %s for more information!",
                        worker.getAddress(), processManager.getPublicAddress(), worker.getWorkerHome()));
            }

            String pid = readPid(worker);
            if (pid != null) {
                LOGGER.info(format("Worker %s started", worker.getId()));
                return;
            }

            sleepMillis(WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS);
        }

        throw new CreateWorkerFailedException(format(
                "Worker %s on Agent %s didn't start within %s seconds, check log files in %s for more information!",
                worker.getAddress(), processManager.getPublicAddress(), timeout, worker.getWorkerHome()));
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

    private void copyResourcesToWorkerHome(String workerId) {
        File workersHome = new File(getSimulatorHome(), WORKERS_HOME_NAME);
        String sessionId = parameters.get("SESSION_ID");
        File uploadDirectory = new File(workersHome, sessionId + "/upload/").getAbsoluteFile();
        if (!uploadDirectory.exists() || !uploadDirectory.isDirectory()) {
            LOGGER.debug("Skip copying upload directory to workers since no upload directory was found");
            return;
        }

        String copyCommand = format("cp -rfv %s/%s/upload %s/%s/%s/ || true",
                workersHome,
                sessionId,
                workersHome,
                sessionId,
                workerId);
        execute(copyCommand);
        LOGGER.info(format("Finished copying '%s' to Worker", uploadDirectory));
    }

    private boolean hasExited(WorkerProcess workerProcess) {
        try {
            workerProcess.getProcess().exitValue();
            return true;
        } catch (IllegalThreadStateException e) {
            return false;
        }
    }

    private String readPid(WorkerProcess workerProcess) {
        File pidFile = new File(workerProcess.getWorkerHome(), "worker.pid");
        if (!pidFile.exists()) {
            return null;
        }

        return fileAsText(pidFile);
    }

    private String getClasspath(File workerHome) {
        String simulatorHome = getSimulatorHome().getAbsolutePath();
        String classpath = new File(getSessionDirectory(), "lib/*").getAbsolutePath();
        if (parameters.get("VERSION_SPEC").equals("bringmyown")) {
            classpath += CLASSPATH_SEPARATOR + workerHome.getAbsolutePath() + "/upload/*";
        }
        classpath += CLASSPATH_SEPARATOR + simulatorHome + "/user-lib/*"
                + uploadDirToClassPath(workerHome)
                + CLASSPATH_SEPARATOR + CLASSPATH;

        String driver = parameters.get("DRIVER");
        if ("hazelcast3".equals(driver) || "hazelcast-enterprise3".equals(driver)) {
            String hzVersionDirectory = directoryForVersionSpec(parameters.get("VERSION_SPEC"));
            classpath += CLASSPATH_SEPARATOR + simulatorHome + "/driver-lib/" + hzVersionDirectory + "/*";
            // the common test classes.
            classpath += CLASSPATH_SEPARATOR + simulatorHome + "/drivers/driver-hazelcast3/*";
        } else if ("hazelcast4".equals(driver) || "hazelcast-enterprise4".equals(driver)) {
            String hzVersionDirectory = directoryForVersionSpec(parameters.get("VERSION_SPEC"));
            classpath += CLASSPATH_SEPARATOR + simulatorHome + "/driver-lib/" + hzVersionDirectory + "/*";
            // the common test classes.
            classpath += CLASSPATH_SEPARATOR + simulatorHome + "/drivers/driver-hazelcast4/*";
        } else {
            classpath += CLASSPATH_SEPARATOR + simulatorHome + "/drivers/driver-" + driver + "/*";
        }

        // This will add all upload directory to classpath in case bringmyown
        // option isn't used but since it will be after drivers the JVM will use
        // correct JARs
        if (!parameters.get("VERSION_SPEC").equals("bringmyown")) {
            classpath += CLASSPATH_SEPARATOR + workerHome.getAbsolutePath() + "/upload/*";
        }

        return classpath;
    }

    private String uploadDirToClassPath(File workerHome) {
        String uploadClassPath = "";
        File uploadDirectory = new File(workerHome, "upload").getAbsoluteFile();
        if (uploadDirectory.exists() && uploadDirectory.isDirectory()) {
            File[] files = uploadDirectory.listFiles();
            if (files != null && files.length > 0) {
                // this adds all jars in the directory
                uploadClassPath = CLASSPATH_SEPARATOR + uploadDirectory.getAbsolutePath() + "/*";
                LOGGER.info(format("Adding upload directory %s to classpath", uploadClassPath));

                // this adds directory with classes to classpath
                // this is a bit of a hack we need to address in the future
                uploadClassPath = CLASSPATH_SEPARATOR + uploadDirectory.getAbsolutePath() + "/";
            }
        }
        return uploadClassPath;
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
