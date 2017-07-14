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

package com.hazelcast.simulator.boot;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.TargetType;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.coordinator.operations.RcTestRunOperation;
import com.hazelcast.simulator.coordinator.operations.RcWorkerStartOperation;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.coordinator.registry.WorkerQuery;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.copyDirectory;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;

public class Runner {

    public static final int PORT_COUNT = 200;
    public static final int PORT = 5701;

    private final Options options;

    public Runner(OptionsBuilder optionsBuilder) {
        this(optionsBuilder.build());
    }

    public Runner(Options options) {
        this.options = options;
    }

    public void run() throws Exception {
        prepareClassPathForUploading();

        Coordinator coordinator = newCoordinator();
        try {
            startMembers(coordinator);
            startClients(coordinator);

            TestSuite suite = newTestSuite();
            coordinator.testRun(new RcTestRunOperation(suite));
        } finally {
            coordinator.close();
        }
    }

    private void prepareClassPathForUploading() {
        List<File> workerClassPath = getWorkerClassPath();
        if (workerClassPath.isEmpty()) {
            return;
        }

        File uploadDir = new File("upload");
        uploadDir.mkdirs();

        for (File file : workerClassPath) {
            copyDirectory(file, uploadDir);
        }
    }

    private Coordinator newCoordinator() {
        CoordinatorParameters parameters = new CoordinatorParameters()
                .setSkipShutdownHook(true)
                .setSimulatorProperties(options.simulatorProperties);

        if (options.sessionId != null) {
            parameters.setSessionId(options.sessionId);
        }

        Registry registry;
        if ("local".equals(options.simulatorProperties.getCloudProvider())) {
            registry = new Registry();
            registry.addAgent("localhost", "localhost");
        } else {
            registry = loadComponentRegister(new File("agents.txt"));
        }

        Coordinator coordinator = new Coordinator(registry, parameters);

        try {
            coordinator.start();
        } catch (Exception e) {
            //todo
            throw new RuntimeException(e);
        }
        return coordinator;
    }

    private void startMembers(Coordinator coordinator) throws Exception {
        String configString = createConfig();

        RcWorkerStartOperation op = new RcWorkerStartOperation()
                .setVmOptions(options.memberArgs)
                .setHzConfig(configString)
                .setCount(options.memberCount);

        System.out.println("Started members: " + coordinator.workerStart(op));
    }

    private String createConfig() {
        Config memberConfig = options.memberConfig;
        if (memberConfig == null) {
            return null;
        }

        memberConfig.getGroupConfig().setName("workers");

        memberConfig.setProperty("hazelcast.phone.home.enabled", "false");

        NetworkConfig networkConfig = memberConfig.getNetworkConfig();
        networkConfig.setPortAutoIncrement(true);
        networkConfig.setPort(PORT);
        networkConfig.setPortCount(PORT_COUNT);
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(true);

        ConfigXmlGenerator generator = new ConfigXmlGenerator(true);

        String configString = generator.generate(memberConfig);

        if (networkConfig.getJoin().getTcpIpConfig().getMembers().isEmpty()) {
            configString = configString.replace("<member-list/>", "<!--MEMBERS-->");
        }

        configString = fixGroup(configString);

        System.out.println("HZ Configuration:\n" + configString);
        return configString;
    }

    private String fixGroup(String generated) {
        int startIndex = generated.indexOf("<group>");
        int endIndex = generated.indexOf("</group>");

        String configString = generated.substring(0, startIndex);
        configString += "  <group><name>workers</name></group>";
        configString += generated.substring(endIndex + "</group>".length());
        return configString;
    }

    private void startClients(Coordinator coordinator) throws Exception {
        if (options.clientCount <= 0) {
            return;
        }

        RcWorkerStartOperation op = new RcWorkerStartOperation()
                .setVmOptions(options.clientArgs)
                .setCount(options.clientCount)
                .setWorkerType("javaclient");
        System.out.println("Started members: " + coordinator.workerStart(op));
    }

    private TestSuite newTestSuite() {
        return new TestSuite()
                .addTest(options.testCase)
                .setWarmupSeconds((int) options.warmupSeconds)
                .setDurationSeconds((int) options.durationSeconds)
                .setWorkerQuery(new WorkerQuery()
                        .setTargetType(TargetType.PREFER_CLIENT));
    }

    private List<File> getWorkerClassPath() {
        String[] classPath = System.getProperty("java.class.path").split(System.getProperty("path.separator"));
        long size = 0;

        List<File> classpath = new LinkedList<File>();
        for (String s : classPath) {
            if (isIgnored(s)) {
                continue;
            }

            File file = new File(s);
            System.out.println(s + " size: " + file.length());
            size += file.length();

            classpath.add(file);
        }

        System.out.println("total size: " + size);
        return classpath;
    }

    private boolean isIgnored(String classFile) {
        for (String ignored : this.options.ignoredClasspath) {
            if (classFile.contains(ignored)) {
                return true;
            }
        }

        return false;
    }
}
