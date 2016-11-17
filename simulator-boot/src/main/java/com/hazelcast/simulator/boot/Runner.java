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
import com.hazelcast.simulator.coordinator.Coordinator;
import com.hazelcast.simulator.coordinator.CoordinatorParameters;
import com.hazelcast.simulator.coordinator.TestSuite;
import com.hazelcast.simulator.protocol.operation.RcTestRunOperation;
import com.hazelcast.simulator.protocol.operation.RcWorkerStartOperation;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.registry.TargetType;
import com.hazelcast.simulator.protocol.registry.WorkerQuery;
import com.hazelcast.simulator.utils.DefaultPromise;
import com.hazelcast.simulator.utils.FileUtils;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;

public class Runner {

    private static final int PERFORMANCE_MONITOR_INTERVAL_SECONDS = 10;

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

            TestSuite suite = newTestSuite()
                    .setWorkerQuery(new WorkerQuery().setTargetType(TargetType.PREFER_CLIENT));

            DefaultPromise promise = new DefaultPromise();
            coordinator.testRun(new RcTestRunOperation(suite), promise);
            promise.get();
        } finally {
            coordinator.close();
        }
    }

    private void prepareClassPathForUploading() {
        List<File> workerClassPath = getWorkerClassPath();
        if (workerClassPath.isEmpty()) {
            return;
        }

        File uploadDir = new File("upload/lib");
        uploadDir.mkdirs();

        for (File file : workerClassPath) {
            FileUtils.copyDirectory(file, uploadDir);
        }
    }

    private Coordinator newCoordinator() {
        CoordinatorParameters parameters = new CoordinatorParameters()
                .setPerformanceMonitorIntervalSeconds(PERFORMANCE_MONITOR_INTERVAL_SECONDS)
                .setSimulatorProperties(options.simulatorProperties);

        if (options.sessionId != null) {
            parameters.setSessionId(options.sessionId);
        }

        ComponentRegistry componentRegistry;
        if ("local".equals(options.simulatorProperties.getCloudProvider())) {
            componentRegistry = new ComponentRegistry();
            componentRegistry.addAgent("localhost", "localhost");
        } else {
            componentRegistry = loadComponentRegister(new File("agents.txt"));
        }

        Coordinator coordinator = new Coordinator(componentRegistry, parameters);

        coordinator.start();
        return coordinator;
    }

    private void startMembers(Coordinator coordinator) throws Exception {
        String configString = createConfig();

        RcWorkerStartOperation op = new RcWorkerStartOperation()
                .setVmOptions(options.memberVmOptions)
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

        ConfigXmlGenerator generator = new ConfigXmlGenerator(true);
        String generated = generator.generate(memberConfig);

        String configString = fixNetwork(generated);
        configString = fixGroup(configString);

        System.out.println("HZ Configuration:\n" + configString);
        return configString;
    }

    private String fixNetwork(String generated) {
        int startIndex = generated.indexOf("<network>");
        int endIndex = generated.indexOf("</network>");

        String configString = generated.substring(0, startIndex);
        configString += "  <network>\n"
                + "        <port port-count=\"200\" auto-increment=\"true\">5701</port>\n"
                + "        <join>\n" + "            <multicast enabled=\"false\"/>\n"
                + "            <tcp-ip enabled=\"true\">\n"
                + "                <!--MEMBERS-->\n"
                + "            </tcp-ip>\n"
                + "        </join>\n"
                + "    </network>";
        configString += generated.substring(endIndex + "</network>".length());
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
                .setVmOptions(options.clientVmOptions)
                .setCount(options.clientCount)
                .setWorkerType("javaclient");
        System.out.println("Started members: " + coordinator.workerStart(op));
    }

    private TestSuite newTestSuite() {
        return new TestSuite()
                .setDurationSeconds((int) options.durationSeconds)
                .addTest(options.testCase);
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
