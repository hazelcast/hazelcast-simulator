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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import com.hazelcast.simulator.utils.jars.HazelcastJARs;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

import static com.hazelcast.simulator.coordinator.Coordinator.SIMULATOR_HOME;
import static com.hazelcast.simulator.coordinator.Coordinator.SIMULATOR_VERSION;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.FileUtils.getFilesFromClassPath;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static java.lang.String.format;

class CoordinatorUploader {

    private static final File WORKING_DIRECTORY = new File(System.getProperty("user.dir"));
    private static final File UPLOAD_DIRECTORY = new File(WORKING_DIRECTORY, "upload");

    private static final Logger LOGGER = Logger.getLogger(CoordinatorUploader.class);

    private final String simulatorHome = SIMULATOR_HOME.getAbsolutePath();

    private final ComponentRegistry componentRegistry;
    private final Bash bash;

    private final HazelcastJARs hazelcastJARs;
    private final boolean isEnterpriseEnabled;

    private final String testSuiteId;
    private final String workerClassPath;

    private final JavaProfiler javaProfiler;

    public CoordinatorUploader(ComponentRegistry componentRegistry, Bash bash, String testSuiteId, HazelcastJARs hazelcastJARs,
                               boolean isEnterpriseEnabled, String workerClassPath, JavaProfiler javaProfiler) {
        this.componentRegistry = componentRegistry;
        this.bash = bash;

        this.hazelcastJARs = hazelcastJARs;
        this.isEnterpriseEnabled = isEnterpriseEnabled;

        this.testSuiteId = testSuiteId;
        this.workerClassPath = workerClassPath;

        this.javaProfiler = javaProfiler;
    }

    public void run() {
        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info("Uploading files to agents...");
        LOGGER.info(HORIZONTAL_RULER);

        long started = System.nanoTime();
        uploadHazelcastJARs();
        uploadUploadDirectory();
        uploadWorkerClassPath();
        uploadYourKit();
        long elapsed = getElapsedSeconds(started);

        LOGGER.info(HORIZONTAL_RULER);
        LOGGER.info(format("Finished upload of files to agents (%d seconds)", elapsed));
        LOGGER.info(HORIZONTAL_RULER);
    }

    void uploadHazelcastJARs() {
        LOGGER.info("Preparing Hazelcast JARs...");
        hazelcastJARs.prepare(isEnterpriseEnabled);

        LOGGER.info("Uploading Hazelcast JARs...");
        ThreadSpawner spawner = new ThreadSpawner("uploadHazelcastJARs", true);
        long started = System.nanoTime();
        for (AgentData agentData : componentRegistry.getAgents()) {
            final String ip = agentData.getPublicAddress();
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    hazelcastJARs.upload(ip, simulatorHome);
                    logAgentDone(ip);
                }
            });
        }
        spawner.awaitCompletion();
        LOGGER.info(format("Finished upload of Hazelcast JARs to agents (%d seconds)", getElapsedSeconds(started)));
    }

    void uploadUploadDirectory() {
        try {
            if (!UPLOAD_DIRECTORY.exists()) {
                return;
            }

            String sourcePath = UPLOAD_DIRECTORY.getAbsolutePath();
            final String targetPath = format("workers/%s/", testSuiteId);
            final List<File> sourceFiles = getFilesFromClassPath(sourcePath);

            LOGGER.info(format("Starting uploading '%s' to agents", sourcePath));
            ThreadSpawner spawner = new ThreadSpawner("uploadUploadDirectory", true);
            long started = System.nanoTime();
            for (AgentData agentData : componentRegistry.getAgents()) {
                final String ip = agentData.getPublicAddress();
                spawner.spawn(new Runnable() {
                    @Override
                    public void run() {
                        for (File sourceFile : sourceFiles) {
                            bash.uploadToRemoteSimulatorDir(ip, sourceFile.getAbsolutePath(), targetPath);
                        }
                        logAgentDone(ip);
                    }
                });
            }
            spawner.awaitCompletion();
            LOGGER.info(format("Finished uploading '%s' to agents (%d seconds)", sourcePath, getElapsedSeconds(started)));
        } catch (Exception e) {
            throw new CommandLineExitException("Could not copy upload directory to agents", e);
        }
    }

    void uploadWorkerClassPath() {
        if (workerClassPath == null) {
            return;
        }

        try {
            final String targetPath = format("workers/%s/lib/", testSuiteId);
            final List<File> sourceFiles = getFilesFromClassPath(workerClassPath);

            LOGGER.info(format("Copying %d files from workerClasspath '%s' to agents", sourceFiles.size(), workerClassPath));
            ThreadSpawner spawner = new ThreadSpawner("uploadWorkerClassPath", true);
            long started = System.nanoTime();
            for (AgentData agentData : componentRegistry.getAgents()) {
                final String ip = agentData.getPublicAddress();
                spawner.spawn(new Runnable() {
                    @Override
                    public void run() {
                        for (File sourceFile : sourceFiles) {
                            bash.uploadToRemoteSimulatorDir(ip, sourceFile.getAbsolutePath(), targetPath);
                        }
                        logAgentDone(ip);
                    }
                });
            }
            spawner.awaitCompletion();
            long elapsed = getElapsedSeconds(started);
            LOGGER.info(format("Finished copying workerClasspath '%s' to agents (%d seconds)", workerClassPath, elapsed));
        } catch (Exception e) {
            throw new CommandLineExitException("Could not upload worker classpath to agents", e);
        }
    }

    void uploadYourKit() {
        if (javaProfiler != JavaProfiler.YOURKIT) {
            return;
        }

        // TODO: only upload the requested YourKit library (32 or 64 bit)
        LOGGER.info("Uploading YourKit dependencies...");
        ThreadSpawner spawner = new ThreadSpawner("uploadYourKit", true);
        long started = System.nanoTime();
        for (AgentData agentData : componentRegistry.getAgents()) {
            final String ip = agentData.getPublicAddress();
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/yourkit", SIMULATOR_VERSION));
                    bash.uploadToRemoteSimulatorDir(ip, simulatorHome + "/yourkit/", "yourkit");
                    logAgentDone(ip);
                }
            });
        }
        spawner.awaitCompletion();
        LOGGER.info(format("Finished upload of YourKit to agents (%d seconds)", getElapsedSeconds(started)));
    }

    private void logAgentDone(String ip) {
        LOGGER.info("    Agent " + ip + " done");
    }
}
