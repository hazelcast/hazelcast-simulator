/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.utils.CommonUtils.sleepMillis;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingDirectory;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.readObject;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.NativeUtils.execute;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.directoryForVersionSpec;
import static java.lang.String.format;
import static java.util.Arrays.asList;

public class WorkerJvmLauncher {

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

            String hzConfigFileName = (type == WorkerType.MEMBER) ? "hazelcast" : "client-hazelcast";
            hzConfigFile = createTmpXmlFile(hzConfigFileName, workerJvmSettings.getHazelcastConfig());
            log4jFile = createTmpXmlFile("worker-log4j", workerJvmSettings.getLog4jConfig());
            LOGGER.info("Spawning Worker JVM using settings: " + workerJvmSettings);

            WorkerJvm worker = startWorkerJvm();
            LOGGER.info(format("Finished starting a JVM for %s Worker #%d", type, workerIndex));

            waitForWorkersStartup(worker, workerJvmSettings.getWorkerStartupTimeout());
        } catch (Exception e) {
            LOGGER.error("Failed to start Worker", e);
            throw new SpawnWorkerFailedException("Failed to start Worker", e);
        }
    }

    private File createTmpXmlFile(String name, String content) throws IOException {
        File tmpXmlFile = File.createTempFile(name, ".xml");
        tmpXmlFile.deleteOnExit();
        writeText(content, tmpXmlFile);

        return tmpXmlFile;
    }

    private WorkerJvm startWorkerJvm() throws IOException {
        int workerIndex = workerJvmSettings.getWorkerIndex();
        WorkerType type = workerJvmSettings.getWorkerType();

        SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, agent.getAddressIndex(), workerIndex, 0);
        String workerId = "worker-" + agent.getPublicAddress() + '-' + workerIndex + '-' + type.toLowerCase();
        File workerHome = new File(testSuiteDir, workerId);
        ensureExistingDirectory(workerHome);

        WorkerJvm workerJvm = new WorkerJvm(workerAddress, workerId, workerHome);

        generateWorkerStartScript(type, workerJvm);

        ProcessBuilder processBuilder = new ProcessBuilder(new String[]{"bash", "worker.sh"})
                .directory(workerHome)
                .redirectErrorStream(true);

        Map<String, String> environment = processBuilder.environment();
        String javaHome = getJavaHome();
        String path = javaHome + File.pathSeparator + "bin:" + environment.get("PATH");
        environment.put("PATH", path);
        environment.put("JAVA_HOME", javaHome);

        Process process = processBuilder.start();
        workerJvm.setProcess(process);
        copyResourcesToWorkerId(workerId);
        workerJvmManager.add(workerAddress, workerJvm);

        return workerJvm;
    }

    private void waitForWorkersStartup(WorkerJvm worker, int workerTimeoutSec) {
        for (int i = 0; i < workerTimeoutSec; i++) {
            if (hasExited(worker)) {
                throw new SpawnWorkerFailedException(format(
                        "Startup of Worker on host %s failed, check log files in %s for more information!",
                        agent.getPublicAddress(), worker.getWorkerHome()));
            }

            String address = readAddress(worker);
            if (address != null) {
                worker.setHzAddress(address);
                LOGGER.info(format("Worker %s started", worker.getId()));
                return;
            }

            sleepMillis(WAIT_FOR_WORKER_STARTUP_INTERVAL_MILLIS);
        }

        throw new SpawnWorkerFailedException(format("Worker %s of Testsuite %s on Agent %s didn't start within %s seconds",
                worker.getId(), agent.getTestSuite().getId(), agent.getPublicAddress(), workerTimeoutSec));
    }

    private String getJavaHome() {
        String javaHome = System.getProperty("java.home");
        if (javaHomePrinted.compareAndSet(false, true)) {
            LOGGER.info("java.home=" + javaHome);
        }
        return javaHome;
    }

    private void generateWorkerStartScript(WorkerType type, WorkerJvm workerJvm) {
        String[] args = buildArgs(workerJvm, type);
        File startScript = new File(workerJvm.getWorkerHome(), "worker.sh");

        StringBuilder sb = new StringBuilder();
        sb.append("#!/bin/bash").append(NEW_LINE);
        for (String arg : args) {
            sb.append(arg).append(' ');
        }
        sb.append("> worker.out 2> worker.err").append(NEW_LINE);

        writeText(sb.toString(), startScript);
    }

    private void copyResourcesToWorkerId(String workerId) {
        File workersDir = new File(getSimulatorHome(), "workers");
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

        String address = readObject(file);
        deleteQuiet(file);

        return address;
    }

    private String[] buildArgs(WorkerJvm workerJvm, WorkerType type) {
        List<String> args = new LinkedList<String>();

        int workerIndex = workerJvmSettings.getWorkerIndex();
        int workerPort = agent.getPort() + workerIndex;

        addNumaCtlSettings(args);
        addProfilerSettings(workerJvm, args);

        args.add("-classpath");
        args.add(getClasspath());
        args.addAll(getJvmOptions());
        args.add("-XX:OnOutOfMemoryError=\"touch worker.oome\"");
        args.add("-Dhazelcast.logging.type=log4j");
        args.add("-Dlog4j.configuration=file:" + log4jFile.getAbsolutePath());

        args.add("-DSIMULATOR_HOME=" + getSimulatorHome());
        args.add("-DworkerId=" + workerJvm.getId());
        args.add("-DworkerType=" + type);
        args.add("-DpublicAddress=" + agent.getPublicAddress());
        args.add("-DagentIndex=" + agent.getAddressIndex());
        args.add("-DworkerIndex=" + workerIndex);
        args.add("-DworkerPort=" + workerPort);
        args.add("-DautoCreateHzInstance=" + workerJvmSettings.isAutoCreateHzInstance());
        args.add("-DworkerPerformanceMonitorIntervalSeconds=" + workerJvmSettings.getWorkerPerformanceMonitorIntervalSeconds());
        args.add("-DhzConfigFile=" + hzConfigFile.getAbsolutePath());

        // add class name to start correct worker type
        args.add(type.getClassName());

        return args.toArray(new String[args.size()]);
    }

    private void addNumaCtlSettings(List<String> args) {
        String numaCtl = workerJvmSettings.getNumaCtl();
        if (!"none".equals(numaCtl)) {
            args.add(numaCtl);
        }
    }

    private void addProfilerSettings(WorkerJvm workerJvm, List<String> args) {
        String javaExecutable = "java";
        switch (workerJvmSettings.getProfiler()) {
            case YOURKIT:
                args.add(javaExecutable);
                String agentSetting = workerJvmSettings.getProfilerSettings()
                        .replace("${SIMULATOR_HOME}", getSimulatorHome().getAbsolutePath())
                        .replace("${WORKER_HOME}", workerJvm.getWorkerHome().getAbsolutePath());
                args.add(agentSetting);
                break;
            case FLIGHTRECORDER:
            case HPROF:
                args.add(javaExecutable);
                args.add(workerJvmSettings.getProfilerSettings());
                break;
            case PERF:
            case VTUNE:
                // perf and vtune command always need to be in front of the java command
                args.add(workerJvmSettings.getProfilerSettings());
                args.add(javaExecutable);
                break;
            default:
                args.add(javaExecutable);
        }
    }

    private String getClasspath() {
        String hzVersionDirectory = directoryForVersionSpec(workerJvmSettings.getHazelcastVersionSpec());
        return CLASSPATH
                + CLASSPATH_SEPARATOR + getSimulatorHome() + "/hz-lib/" + hzVersionDirectory + "/*"
                + CLASSPATH_SEPARATOR + getSimulatorHome() + "/user-lib/*"
                + CLASSPATH_SEPARATOR + new File(agent.getTestSuiteDir(), "lib/*").getAbsolutePath();
    }

    private List<String> getJvmOptions() {
        String workerVmOptions = workerJvmSettings.getJvmOptions();

        String[] vmOptionsArray = new String[]{};
        if (workerVmOptions != null && !workerVmOptions.trim().isEmpty()) {
            vmOptionsArray = workerVmOptions.split("\\s+");
        }
        return asList(vmOptionsArray);
    }
}
