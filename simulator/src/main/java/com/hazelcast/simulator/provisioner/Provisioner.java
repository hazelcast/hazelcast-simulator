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
package com.hazelcast.simulator.provisioner;

import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.coordinator.AgentUtils.onlineCheckAgents;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsCloudProviderSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsRemoteSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.getInitScriptFile;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getConfigurationFile;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.FormatUtils.join;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUuidString;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Provisioner {

    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;
    private static final String INDENTATION = "    ";
    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);

    private final String simulatorPath = getSimulatorHome().getAbsolutePath();
    private final File agentsFile = new File(getUserDir(), AgentsFile.NAME);
    private final ExecutorService executor = newFixedThreadPool(10);
    private final SimulatorProperties properties;
    private final Bash bash;
    private final Registry registry;
    private final File initScriptFile;

    public Provisioner(SimulatorProperties properties, Bash bash) {
        this.properties = properties;
        this.bash = bash;
        this.registry = loadComponentRegister(agentsFile, false);
        this.initScriptFile = getInitScriptFile(simulatorPath);
    }

    // just for testing
    Registry getRegistry() {
        return registry;
    }

    void installJava(String jdkUrl) {
        if (jdkUrl != null) {
            properties.set("JDK_URL", jdkUrl);
        }
        onlineCheckAgents(properties, registry);
        ensureIsRemoteSetup(properties, "installJava");

        long started = System.nanoTime();
        logWithRuler("Installing Java on %d machines...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installJava", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(() -> {
                log("Installing Java on %s", agent.getPublicAddress());
                installJavaOnAgent(agent.getPublicAddress());
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Finished installing Java on %d machines (%s seconds)", registry.agentCount(), elapsed);
    }

    private void installJavaOnAgent(String ip) {
        String jdkUrl = properties.get("JDK_URL");
        if (jdkUrl != null) {
            File file = new File(new File(simulatorPath, "jdk-install"), "jdk_url.sh");
            String content = FileUtils.fileAsText(file);
            content = content.replace("%JDK_URL%", jdkUrl);
            file = FileUtils.newTmpFile(content);
            bash.scpToRemote(ip, getJavaSupportScript(), "jdk-support.sh");
            bash.scpToRemote(ip, file, "install-java.sh");
            bash.ssh(ip, "bash install-java.sh");
            return;
        }

        if ("outofthebox".equals(properties.getJdkFlavor())) {
            return;
        }

        bash.scpToRemote(ip, getJavaSupportScript(), "jdk-support.sh");
        bash.scpToRemote(ip, getJavaInstallScript(), "install-java.sh");
        bash.ssh(ip, "bash install-java.sh");
    }

    private File getJavaInstallScript() {
        String flavor = properties.getJdkFlavor();
        String version = properties.getJdkVersion();
        String script = "jdk-" + flavor + '-' + version + "-64.sh";
        File scriptDir = new File(simulatorPath, "jdk-install");
        return new File(scriptDir, script);
    }

    private File getJavaSupportScript() {
        File scriptDir = new File(simulatorPath, "jdk-install");
        return new File(scriptDir, "jdk-support.sh");
    }

    void installSimulator() {
        onlineCheckAgents(properties, registry);
        ensureIsRemoteSetup(properties, "install");

        long started = System.nanoTime();
        logWithRuler("Installing Simulator on %d machines...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installSimulator", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(() -> {
                log("    Installing Simulator on %s", agent.getPublicAddress());
                installSimulator(agent.getPublicAddress());
                log("    Finished installing Simulator on %s", agent.getPublicAddress());
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Finished installing Simulator on %d machines (%s seconds)", registry.agentCount(), elapsed);
    }

    void killJavaProcesses(final boolean sudo) {
        onlineCheckAgents(properties, registry);
        ensureIsRemoteSetup(properties, "kill");

        long started = System.nanoTime();
        logWithRuler("Killing %s Java processes...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("killJavaProcesses", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(() -> {
                log("    Killing Java processes on %s", agent.getPublicAddress());
                bash.killAllJavaProcesses(agent.getPublicAddress(), sudo);
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Successfully killed %s Java processes (%s seconds)", registry.agentCount(), elapsed);
    }

    void terminate() {
        ensureIsCloudProviderSetup(properties, "terminate");

        scaleDown(Integer.MAX_VALUE);
    }

    public void scale(int size, Map<String, String> tags) {
        ensureIsCloudProviderSetup(properties, "scale");

        int agentSize = registry.agentCount();
        int delta = size - agentSize;
        if (delta == 0) {
            log("Current number of machines: " + agentSize);
            log("Desired number of machines: " + (agentSize + delta));
            log("Ignoring spawn machines, desired number of machines already exists.");
        } else if (delta > 0) {
            scaleUp(delta, tags);
        } else {
            scaleDown(-delta);
        }
    }

    void shutdown() {
        log("Shutting down Provisioner...");

        // shutdown thread pool
        executor.shutdown();
        awaitTermination(executor, EXECUTOR_TERMINATION_TIMEOUT_SECONDS, SECONDS);

        log("Done!");
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    private void scaleUp(int delta, Map<String, String> tags) {
        logWithRuler("Provisioning %s %s machines", delta, properties.getCloudProvider());
        log("Current number of machines: " + registry.agentCount());
        log("Desired number of machines: " + (registry.agentCount() + delta));

        String groupName = properties.get("GROUP_NAME", "simulator-agent");
        log("GroupName: " + groupName);
        log("Username: " + properties.getUser());
        log("Using init script: " + initScriptFile.getAbsolutePath());

        String jdkFlavor = properties.getJdkFlavor();
        if ("outofthebox".equals(jdkFlavor)) {
            log("JDK spec: outofthebox");
        } else {
            String jdkVersion = properties.getJdkVersion();
            log("JDK spec: %s %s", jdkFlavor, jdkVersion);
        }

        long started = System.nanoTime();
        try {
            log("Creating machines (can take a few minutes)...");
            String filename = properties.get("CLOUD_PROVIDER") + "_provision.sh";
            new BashCommand(getConfigurationFile(filename).getAbsolutePath())
                    .addEnvironment(properties.asMap())
                    .addParams(delta)
                    .execute();
            String[] agentLines = fileAsText(agentsFile).split("\n");
            Set<Future> futures = new HashSet<>();
            for (int k = 0; k < delta; k++) {
                String agentLine = agentLines[k + registry.agentCount()];
                String publicIpAddress = agentLine.split(",")[0];
                Future future = executor.submit(new InstallNodeTask(publicIpAddress));
                futures.add(future);
            }
            for (Future future : futures) {
                future.get();
            }
        } catch (Exception e) {
            LOGGER.error("Failed to provision machines", e);
            throw new CommandLineExitException("Failed to provision machines: " + e.getMessage());
        }

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Successfully provisioned %s %s machines (%s seconds)", delta, properties.getCloudProvider(), elapsed);
    }

    private void scaleDown(int count) {
        if (count > registry.agentCount()) {
            count = registry.agentCount();
        }

        logWithRuler("Terminating %s %s machines (can take some time)", count, properties.getCloudProvider());
        log("Current number of machines: " + registry.agentCount());
        log("Desired number of machines: " + (registry.agentCount() - count));

        long started = System.nanoTime();

        int destroyedCount = 0;
        List<String> privateIps = new LinkedList<>();
        List<String> publicIps = new LinkedList<>();
        List<AgentData> agents = registry.getAgents();
        for (int k = 0; k < count; k++) {
            AgentData agent = agents.get(k);
            privateIps.add(agent.getPrivateAddress());
            publicIps.add(agent.getPublicAddress());
            destroyedCount++;
        }

        String cloudProvider = properties.get("CLOUD_PROVIDER");
        new BashCommand(getConfigurationFile(cloudProvider + "_terminate.sh").getAbsolutePath())
                .addEnvironment(properties.asMap())
                .addParams(join(privateIps, ","))
                .addParams(join(publicIps, ","))
                .execute();

        for (int k = 0; k < count; k++) {
            registry.removeAgent(agents.get(k));
        }

        log("Updating " + agentsFile.getAbsolutePath());
        AgentsFile.save(agentsFile, registry);

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Terminated %s of %s machines (%s remaining) (%s seconds)", destroyedCount, count,
                registry.agentCount(), elapsed);

        if (destroyedCount != count) {
            throw new IllegalStateException("Terminated " + destroyedCount + " of " + count
                    + NEW_LINE + "1) You are trying to terminate physical hardware that you own (unsupported feature)"
                    + NEW_LINE + "2) You have not payed you bill and your instances have been terminated by your cloud provider"
                    + NEW_LINE + "3) You have terminated your own instances (perhaps via some console interface)"
                    + NEW_LINE + "4) Someone else has terminated your instances"
                    + NEW_LINE + "Please try again!");
        }
    }

    private void installSimulator(String ip) {
        new BashCommand(getConfigurationFile("install-simulator.sh").getAbsolutePath())
                .addEnvironment(properties.asMap())
                .addParams(ip)
                .execute();

        // execute the init.sh script
        File initFile = newFile("init-" + newUnsecureUuidString() + ".sh");
        writeText(loadInitScript(), initFile);
        bash.scpToRemote(ip, initFile, "init.sh");
        bash.sshTTY(ip, "bash init.sh");
        deleteQuiet(initFile);
    }

    private String loadInitScript() {
        return fileAsText(initScriptFile)
                .replaceAll(Pattern.quote("${version}"), getSimulatorVersion())
                .replaceAll(Pattern.quote("${user}"), properties.getUser())
                .replaceAll(Pattern.quote("${cloudprovider}"), properties.getCloudProvider());
    }

    private static void log(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private static void logWithRuler(String message, Object... args) {
        log(HORIZONTAL_RULER);
        log(message, args);
        log(HORIZONTAL_RULER);
    }

    private final class InstallNodeTask implements Runnable {

        private final String ip;

        private InstallNodeTask(String ip) {
            this.ip = ip;
        }

        @Override
        public void run() {
            if (!"outofthebox".equals(properties.getJdkFlavor())) {
                log(INDENTATION + ip + " Java installation started...");
                installJavaOnAgent(ip);
                log(INDENTATION + ip + " Java Installed");
            }
            log(INDENTATION + ip + " Simulator installation started...");
            installSimulator(ip);
            log(INDENTATION + ip + " Simulator installed");
        }
    }
}
