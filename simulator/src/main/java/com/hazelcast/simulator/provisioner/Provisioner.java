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
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import static com.hazelcast.simulator.coordinator.AgentUtils.sslTestAgents;
import static com.hazelcast.simulator.harakiri.HarakiriMonitorUtils.getStartHarakiriMonitorCommandOrNull;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsCloudProviderSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.ensureIsRemoteSetup;
import static com.hazelcast.simulator.provisioner.ProvisionerUtils.getInitScriptFile;
import static com.hazelcast.simulator.utils.CommonUtils.awaitTermination;
import static com.hazelcast.simulator.utils.CommonUtils.getElapsedSeconds;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
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
import static java.util.concurrent.TimeUnit.SECONDS;

class Provisioner {

    private static final int MACHINE_WARMUP_WAIT_SECONDS = 10;
    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;

    private static final String INDENTATION = "    ";

    private static final Logger LOGGER = Logger.getLogger(Provisioner.class);
    private final String simulatorPath = getSimulatorHome().getAbsolutePath();

    private final File agentsFile = new File(getUserDir(), AgentsFile.NAME);
    private final ExecutorService executor = Executors.newFixedThreadPool(10);

    private final SimulatorProperties properties;
    private final Bash bash;

    private final int machineWarmupSeconds;

    private final Registry registry;
    private final File initScriptFile;

    public Provisioner(SimulatorProperties properties, Bash bash) {
        this(properties, bash, MACHINE_WARMUP_WAIT_SECONDS);
    }

    public Provisioner(SimulatorProperties properties, Bash bash, int machineWarmupSeconds) {
        this.properties = properties;
        this.bash = bash;
        this.machineWarmupSeconds = machineWarmupSeconds;
        this.registry = loadComponentRegister(agentsFile, false);
        this.initScriptFile = getInitScriptFile(simulatorPath);
    }

    // just for testing
    Registry getRegistry() {
        return registry;
    }


    void installJava() {
        sslTestAgents(properties, registry);
        ensureIsRemoteSetup(properties, "installJava");

        long started = System.nanoTime();
        logWithRuler("Installing Java on %d machines...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installJava", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    log("Installing Java on %s", agent.getPublicAddress());
                    uploadJava(agent.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Finished installing Java on %d machines (%s seconds)", registry.agentCount(), elapsed);
    }

    void installSimulator() {
        sslTestAgents(properties, registry);
        ensureIsRemoteSetup(properties, "install");

        long started = System.nanoTime();
        logWithRuler("Installing Simulator on %d machines...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("installSimulator", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    log("    Installing Simulator on %s", agent.getPublicAddress());
                    uploadJARs(agent.getPublicAddress());
                    log("    Finished installing Simulator on %s", agent.getPublicAddress());
                }
            });
        }
        spawner.awaitCompletion();

        long elapsed = getElapsedSeconds(started);
        logWithRuler("Finished installing Simulator on %d machines (%s seconds)", registry.agentCount(), elapsed);
    }

    void killJavaProcesses(final boolean sudo) {
        sslTestAgents(properties, registry);
        ensureIsRemoteSetup(properties, "kill");

        long started = System.nanoTime();
        logWithRuler("Killing %s Java processes...", registry.agentCount());

        ThreadSpawner spawner = new ThreadSpawner("killJavaProcesses", true);
        for (final AgentData agent : registry.getAgents()) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    log("Killing Java processes on %s", agent.getPublicAddress());
                    bash.killAllJavaProcesses(agent.getPublicAddress(), sudo);
                }
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

    void scale(int size, Map<String, String> tags) {
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
        String startHarakiriMonitorCommand = getStartHarakiriMonitorCommandOrNull(properties);
        try {
            log("Creating machines (can take a few minutes)...");
//            Set<Future> futures = new HashSet<Future>();
//            for (int batch : calcBatches(properties, delta)) {
//
//
//                Set<? extends NodeMetadata> nodes = computeService.createNodesInGroup(groupName, batch, template);
//                for (NodeMetadata node : nodes) {
//                    String privateIpAddress = node.getPrivateAddresses().iterator().next();
//                    String publicIpAddress = node.getPublicAddresses().iterator().next();
//
//                    echo(INDENTATION + publicIpAddress + " LAUNCHED");
//                    componentRegistry.addAgent(publicIpAddress, privateIpAddress, tags);
//                    AgentsFile.save(agentsFile, componentRegistry);
//                }
//
//                for (NodeMetadata node : nodes) {
//                    String publicIpAddress = node.getPublicAddresses().iterator().next();
//                    Future future = executor.submit(new InstallNodeTask(publicIpAddress, startHarakiriMonitorCommand));
//                    futures.add(future);
//                }
//            }
//
//            for (Future future : futures) {
//                future.get();
//            }

            new BashCommand(getConfigurationFile("aws-ec2_provision.sh").getAbsolutePath())
                    .addEnvironment(properties.asMap())
                    .addParams(delta)
                    .execute();
        } catch (Exception e) {
            throw new CommandLineExitException("Failed to provision machines: " + e.getMessage());
        }

        log("Pausing for machine warmup... (%d sec)", machineWarmupSeconds);
        sleepSeconds(machineWarmupSeconds);

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
        List<String> privateIps = new LinkedList<String>();
        List<AgentData> agents = registry.getAgents();
        for (int k = 0; k < count; k++) {
            privateIps.add(agents.get(k).getPrivateAddress());
            destroyedCount++;
        }

        new BashCommand(getConfigurationFile("aws-ec2_terminate.sh").getAbsolutePath())
                .addEnvironment(properties.asMap())
                .addParams(join(privateIps, ","))
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

        // first we wipe out the directory if it exists. This way we can start with a clean slate.
        bash.ssh(ip, format("rm -fr hazelcast-simulator-%s", simulatorVersion));

        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/lib/", simulatorVersion));
        bash.ssh(ip, format("mkdir -p hazelcast-simulator-%s/user-lib/", simulatorVersion));

        // delete the old lib folder to prevent different versions of the same JAR to bite us
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/lib/*", simulatorVersion));

        // delete the old user-lib folder to prevent interference with older setups
        bash.sshQuiet(ip, format("rm -f hazelcast-simulator-%s/user-lib/*", simulatorVersion));

        // upload Simulator JARs
        uploadLibraryJar(ip, "simulator-*");

        // we don't copy all JARs to the agent to increase upload speed, e.g. YourKit is uploaded on demand by the Coordinator

        // activemq libraries
        uploadLibraryJar(ip, "activemq-core*");
        uploadLibraryJar(ip, "geronimo-jms*");
        uploadLibraryJar(ip, "geronimo-j2ee*");
        uploadLibraryJar(ip, "slf4j-api*");

        uploadLibraryJar(ip, "cache-api*");
        uploadLibraryJar(ip, "commons-codec*");
        uploadLibraryJar(ip, "commons-lang3*");
        uploadLibraryJar(ip, "freemarker*");
        uploadLibraryJar(ip, "gson-*");
        uploadLibraryJar(ip, "HdrHistogram-*");
        uploadLibraryJar(ip, "jopt*");
        uploadLibraryJar(ip, "junit*");
        uploadLibraryJar(ip, "log4j*");
        uploadLibraryJar(ip, "slf4j-log4j12-*");

        // hack to get ignite working
        if (properties.get("VENDOR").equals("ignite")) {
            uploadLibraryJar(ip, "ignite-*");
            uploadLibraryJar(ip, "spring-*");
            uploadLibraryJar(ip, "commons-logging-*");
        }

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
        bash.sshTTY(ip, "bash init.sh");
        deleteQuiet(initFile);
    }

    private String loadInitScript() {
        String initScript = fileAsText(initScriptFile);

        initScript = initScript.replaceAll(Pattern.quote("${version}"), getSimulatorVersion());
        initScript = initScript.replaceAll(Pattern.quote("${user}"), properties.getUser());
        initScript = initScript.replaceAll(Pattern.quote("${cloudprovider}"), properties.getCloudProvider());

        return initScript;
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
        private final String startHarakiriMonitorCommand;

        private InstallNodeTask(String ip, String startHarakiriMonitorCommand) {
            this.ip = ip;
            this.startHarakiriMonitorCommand = startHarakiriMonitorCommand;
        }

        @Override
        public void run() {
            if (!"outofthebox".equals(properties.getJdkFlavor())) {
                log(INDENTATION + ip + " Java installation started...");
                uploadJava(ip);
                log(INDENTATION + ip + " Java Installed");
            }
            log(INDENTATION + ip + " Simulator installation started...");
            uploadJARs(ip);
            log(INDENTATION + ip + " Simulator installed");

            if (startHarakiriMonitorCommand != null) {
                bash.ssh(ip, startHarakiriMonitorCommand);
                log(INDENTATION + ip + " Harakiri monitor started");
            }
        }
    }
}
