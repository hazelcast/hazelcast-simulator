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
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.Bash;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.provisioner.ProvisionerUtils.calcBatches;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsCloudProviderSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsRemoteSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.getInitScriptFile;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.HORIZONTAL_RULER;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static com.hazelcast.simulator.utils.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static com.hazelcast.simulator.utils.UuidUtil.newUnsecureUuidString;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

class Provisioner {

    private static final int MACHINE_WARMUP_WAIT_SECONDS = 10;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final String INDENTATION = "    ";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);
    private final String simulatorPath = getSimulatorHome().getAbsolutePath();

    private final File agentsFile = new File(getUserDir(), AgentsFile.NAME);
    private final ExecutorService executor = createFixedThreadPool(10, Provisioner.class);

    private final SimulatorProperties properties;
    private final ComputeService computeService;
    private final Bash bash;

    private final int machineWarmupSeconds;

    private final ComponentRegistry componentRegistry;
    private final File initScriptFile;

    public Provisioner(SimulatorProperties properties, ComputeService computeService, Bash bash) {
        this(properties, computeService, bash, MACHINE_WARMUP_WAIT_SECONDS);
    }

    public Provisioner(SimulatorProperties properties, ComputeService computeService, Bash bash, int machineWarmupSeconds) {
        this.properties = properties;
        this.computeService = computeService;
        this.bash = bash;

        this.machineWarmupSeconds = machineWarmupSeconds;

        this.componentRegistry = loadComponentRegister(agentsFile, false);
        this.initScriptFile = getInitScriptFile(simulatorPath);
    }

    // just for testing
    ComponentRegistry getComponentRegistry() {
        return componentRegistry;
    }

    void scale(int size, Map<String, String> tags) {
        ensureIsCloudProviderSetup(properties, "scale");

        int agentSize = componentRegistry.agentCount();
        int delta = size - agentSize;
        if (delta == 0) {
            echo("Current number of machines: " + agentSize);
            echo("Desired number of machines: " + (agentSize + delta));
            echo("Ignoring spawn machines, desired number of machines already exists.");
        } else if (delta > 0) {
            scaleUp(delta, tags);
        } else {
            scaleDown(-delta);
        }
    }

    void installJava() {
        ensureIsRemoteSetup(properties, "installJava");

        long started = System.nanoTime();
        echoImportant("Installing JAVA on %d machines...", componentRegistry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installJava", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Installing JAVA on %s", agentData.getPublicAddress());
                    uploadJava(agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        echoImportant("Finished installing JAVA on %d machines (%s seconds)", componentRegistry.agentCount(), elapsed);
    }

    void installSimulator() {
        ensureIsRemoteSetup(properties, "install");

        long started = System.nanoTime();
        echoImportant("Installing Simulator on %d machines...", componentRegistry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installSimulator", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Installing Simulator on %s", agentData.getPublicAddress());
                    uploadJARs(agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        echoImportant("Finished installing Simulator on %d machines (%s seconds)", componentRegistry.agentCount(), elapsed);
    }

    void killJavaProcesses() {
        ensureIsRemoteSetup(properties, "kill");

        long started = System.nanoTime();
        echoImportant("Killing %s Java processes...", componentRegistry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("killJavaProcesses", true);
        for (final AgentData agentData : componentRegistry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    echo("Killing Java processes on %s", agentData.getPublicAddress());
                    bash.killAllJavaProcesses(agentData.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        echoImportant("Successfully killed %s Java processes (%s seconds)", componentRegistry.agentCount(), elapsed);
    }

    void terminate() {
        ensureIsCloudProviderSetup(properties, "terminate");

        scaleDown(Integer.MAX_VALUE);
    }

    void shutdown() {
        echo("Shutting down Provisioner...");

        // shutdown thread pool
        executor.shutdown();
        awaitTermination(executor, EXECUTOR_TERMINATION_TIMEOUT_SECONDS, SECONDS);

        // shutdown compute service (which holds another thread pool)
        if (computeService != null) {
            computeService.getContext().close();
        }

        echo("Done!");
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    private void scaleUp(int delta, Map<String, String> tags) {
        echoImportant("Provisioning %s %s machines", delta, properties.getCloudProvider());
        echo("Current number of machines: " + componentRegistry.agentCount());
        echo("Desired number of machines: " + (componentRegistry.agentCount() + delta));

        String groupName = properties.get("GROUP_NAME", "simulator-agent");
        echo("GroupName: " + groupName);
        echo("Username: " + properties.getUser());
        echo("Using init script: " + initScriptFile.getAbsolutePath());

        String jdkFlavor = properties.getJdkFlavor();
        if ("outofthebox".equals(jdkFlavor)) {
            echo("JDK spec: outofthebox");
        } else {
            String jdkVersion = properties.getJdkVersion();
            echo("JDK spec: %s %s", jdkFlavor, jdkVersion);
        }

        long started = System.nanoTime();
        Template template = new TemplateBuilder(computeService, properties).build();
        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(properties);

        try {
            echo("Creating machines (can take a few minutes)...");
            Set<Future> futures = new HashSet<Future>();
            for (int batch : calcBatches(properties, delta)) {
                Set<? extends NodeMetadata> nodes = computeService.createNodesInGroup(groupName, batch, template);
                for (NodeMetadata node : nodes) {
                    String privateIpAddress = node.getPrivateAddresses().iterator().next();
                    String publicIpAddress = node.getPublicAddresses().iterator().next();

                    echo(INDENTATION + publicIpAddress + " LAUNCHED");
                    componentRegistry.addAgent(publicIpAddress, privateIpAddress, tags);
                }

                for (NodeMetadata node : nodes) {
                    String publicIpAddress = node.getPublicAddresses().iterator().next();
                    Future future = executor.submit(new InstallNodeTask(publicIpAddress, startHarakiriMonitorCommand));
                    futures.add(future);
                }
            }

            for (Future future : futures) {
                future.get();
            }

            AgentsFile.save(agentsFile, componentRegistry);
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to provision machines: " + e.getMessage());
        }

        echo("Pausing for machine warmup... (%d sec)", machineWarmupSeconds);
        sleepSeconds(machineWarmupSeconds);

        long elapsed = getElapsedSeconds(started);
        echoImportant("Successfully provisioned %s %s machines (%s seconds)", delta, properties.getCloudProvider(), elapsed);
    }

    private void scaleDown(int count) {
        if (count > componentRegistry.agentCount()) {
            count = componentRegistry.agentCount();
        }

        echoImportant("Terminating %s %s machines (can take some time)", count, properties.getCloudProvider());
        echo("Current number of machines: " + componentRegistry.agentCount());
        echo("Desired number of machines: " + (componentRegistry.agentCount() - count));

        long started = System.nanoTime();

        int destroyedCount = 0;
        for (int batchSize : calcBatches(properties, count)) {
            Map<String, AgentData> terminateMap = new HashMap<String, AgentData>();
            for (AgentData agentData : componentRegistry.getAgents(batchSize)) {
                terminateMap.put(agentData.getPublicAddress(), agentData);
            }
            Set destroyedSet = computeService.destroyNodesMatching(new NodeMetadataPredicate(componentRegistry, terminateMap));
            destroyedCount += destroyedSet.size();
        }

        echo("Updating " + agentsFile.getAbsolutePath());
        AgentsFile.save(agentsFile, componentRegistry);

        long elapsed = getElapsedSeconds(started);
        echoImportant("Terminated %s of %s machines (%s remaining) (%s seconds)", destroyedCount, count,
                componentRegistry.agentCount(), elapsed);

        if (destroyedCount != count) {
            throw new IllegalStateException("Terminated " + destroyedCount + " of " + count
                    + NEW_LINE + "1) You are trying to terminate physical hardware that you own (unsupported feature)"
                    + NEW_LINE + "2) If and only if you are using AWS our Harakiri Monitor might have terminated them"
                    + NEW_LINE + "3) You have not payed you bill and your instances have been terminated by your cloud provider"
                    + NEW_LINE + "4) You have terminated your own instances (perhaps via some console interface)"
                    + NEW_LINE + "5) Someone else has terminated your instances"
                    + NEW_LINE + "Please try again!");
        }
    }

    private void uploadJava(String ip) {
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

    private void uploadJARs(String ip) {
        String simulatorVersion = getSimulatorVersion();
        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/lib/", simulatorVersion));
        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/user-lib/", simulatorVersion));

        // delete the old lib folder to prevent different versions of the same JAR to bite us
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/lib/*", simulatorVersion));

        // delete the old user-lib folder to prevent interference with older setups
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/user-lib/*", simulatorVersion));

        // upload Simulator JARs
        uploadLibraryJar(ip, "simulator-*");

        // we don't copy all JARs to the agent to increase upload speed, e.g. YourKit is uploaded on demand by the Coordinator
        uploadLibraryJar(ip, "cache-api*");
        uploadLibraryJar(ip, "commons-codec*");
        uploadLibraryJar(ip, "commons-lang3*");
        uploadLibraryJar(ip, "freemarker*");
        uploadLibraryJar(ip, "gson-*");
        uploadLibraryJar(ip, "guava-*");
        uploadLibraryJar(ip, "HdrHistogram-*");
        uploadLibraryJar(ip, "javassist-*");
        uploadLibraryJar(ip, "jopt*");
        uploadLibraryJar(ip, "junit*");
        uploadLibraryJar(ip, "log4j*");
        uploadLibraryJar(ip, "netty-*");
        uploadLibraryJar(ip, "slf4j-log4j12-*");

        // upload remaining files
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/bin/", "bin");
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/conf/", "conf");
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/jdk-install/", "jdk-install");
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/tests/", "tests");
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/test-lib/", "test-lib/");
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/user-lib/", "user-lib/");

        // purge Hazelcast JARs
        bash.sshQuiet(ip, format("rm -rf hazelcast-simulator-%s/hz-lib", simulatorVersion));

        // execute the init.sh script
        executeInitScript(ip);
    }

    private void uploadLibraryJar(String ip, String jarName) {
        bash.uploadToRemoteSimulatorDir(ip, simulatorPath + "/lib/" + jarName, "lib");
    }

    private void executeInitScript(String ip) {
        File initFile = newFile("init-" + newUnsecureUuidString() + ".sh");
        writeText(loadInitScript(), initFile);
        bash.scpToRemote(ip, initFile, "init.sh");
        bash.ssh(ip, "bash init.sh");
        deleteQuiet(initFile);
    }

    private String loadInitScript() {
        String initScript = fileAsText(initScriptFile);

        initScript = initScript.replaceAll(Pattern.quote("${version}"), getSimulatorVersion());
        initScript = initScript.replaceAll(Pattern.quote("${user}"), properties.getUser());
        initScript = initScript.replaceAll(Pattern.quote("${cloudprovider}"), properties.getCloudProvider());

        return initScript;
    }

    private static void echo(String message, Object... args) {
        LOGGER.info(message == null ? "null" : format(message, args));
    }

    private static void echoImportant(String message, Object... args) {
        echo(HORIZONTAL_RULER);
        echo(message, args);
        echo(HORIZONTAL_RULER);
    }

    private final class InstallNodeTask implements Runnable {

        private final String ip;
        private final String startHarakiriMonitorCommand;

        private InstallNodeTask(String ip, String startHarakiriMonitorCommand) {
            this.ip = ip;
            this.startHarakiriMonitorCommand = startHarakiriMonitorCommand;
        }

        @Override
        public void run() {
            if (!"outofthebox".equals(properties.getJdkFlavor())) {
                echo(INDENTATION + ip + " JAVA INSTALLATION STARTED...");
                uploadJava(ip);
                echo(INDENTATION + ip + " JAVA INSTALLED");
            }
            echo(INDENTATION + ip + " SIMULATOR INSTALLATION STARTED...");
            uploadJARs(ip);
            echo(INDENTATION + ip + " SIMULATOR INSTALLED");

            if (startHarakiriMonitorCommand != null) {
                bash.ssh(ip, startHarakiriMonitorCommand);
                echo(INDENTATION + ip + " HARAKIRI MONITOR STARTED");
            }
        }
    }
}
