/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.TestPhase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CloudProviderUtils;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.TestEnvironmentUtils.createAgentsFileWithLocalhost;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.SimulatorProperties.PROPERTY_CLOUD_PROVIDER;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_STATIC;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.ensureExistingFile;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;

public class CoordinatorCliTest {

    private static final String HAZELCAST_XML = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config"
            + NEW_LINE + "  http://www.hazelcast.com/schema/config/hazelcast-config-3.8.xsd\""
            + NEW_LINE + "  xmlns=\"http://www.hazelcast.com/schema/config\""
            + NEW_LINE + "  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" />";

    private static File testSuiteFile;
    private static File propertiesFile;

    private final List<String> args = new ArrayList<String>();
    private String sessionId;

    @BeforeClass
    public static void beforeClass() {
        setupFakeEnvironment();

        createAgentsFileWithLocalhost();

        testSuiteFile = ensureExistingFile("test.properties");

        appendText("# CoordinatorCliTest", testSuiteFile);

        propertiesFile = ensureExistingFile("simulator.properties");
    }

    @AfterClass
    public static void afterClass() {
        tearDownFakeEnvironment();
        deleteQuiet(testSuiteFile);
        deleteQuiet(propertiesFile);
    }

    @Before
    public void before() {
        sessionId = "CoordinatorCliTest-" + currentTimeMillis();
        args.add("--sessionId");
        args.add(sessionId);
    }

    @After
    public void after() {
        deleteQuiet(new File(getUserDir(), sessionId));
    }

    @Test
    public void testInit() {
        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_withCloudProviderStatic() {
        appendText(format("%s=%s%n", PROPERTY_CLOUD_PROVIDER, PROVIDER_STATIC), propertiesFile);

        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(CoordinatorCli.DEFAULT_DURATION_SECONDS, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testNoTestSuiteAndNoCoordinatorPort() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText("COORDINATOR_PORT=0", simulatorProperties);

        createCoordinatorCli();
    }

    @Test
    public void testInit_duration() {
        args.add("--duration");
        args.add("423");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(423, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withSeconds() {
        args.add("--duration");
        args.add("3s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(TimeUnit.SECONDS.toSeconds(3), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withMinutes() {
        args.add("--duration");
        args.add("5m");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(MINUTES.toSeconds(5), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withHours() {
        args.add("--duration");
        args.add("4h");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(HOURS.toSeconds(4), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withDays() {
        args.add("--duration");
        args.add("23d");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(DAYS.toSeconds(23), testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_duration_withZero() {
        // we are fine with a zero time execution, since it's useful for a dry run
        args.add("--duration");
        args.add("0s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(0, testSuite.getDurationSeconds());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNegativeTime() {
        args.add("--duration");
        args.add("-1");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_duration_withNumberFormatException() {
        args.add("--duration");
        args.add("numberFormatException");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_warmup_withNumberFormatException() {
        args.add("--warmup");
        args.add("numberFormatException");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test
    public void testInit_warmup() {
        args.add("--duration");
        args.add("10s");
        args.add("--warmup");
        args.add("5s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(10, testSuite.getDurationSeconds());
        assertEquals(5, testSuite.getWarmupSeconds());
    }

    @Test
    public void testInit_warmup_withZero() {
        args.add("--warmup");
        args.add("0s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(0, testSuite.getWarmupSeconds());
    }

    @Test
    public void testInit_waitForDuration() {
        args.add("--duration");
        args.add("42s");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        TestSuite testSuite = cli.testSuite;
        assertEquals(42, testSuite.getDurationSeconds());
    }

    @Test
    public void testInit_workerClassPath() {
        args.add("--workerClassPath");
        args.add("*.jar");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals("*.jar", cli.coordinatorParameters.getWorkerClassPath());
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_noWorkersDefined() {
        args.add("--members");
        args.add("0");
        args.add("--clients");
        args.add("0");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_negativeClients() {
        args.add("--members");
        args.add("1");
        args.add("--clients");
        args.add("-1");
        args.add(testSuiteFile.getAbsolutePath());
        createCoordinatorCli();
    }

    @Test
    public void testInit_workersAndClients() {
        args.add("--members");
        args.add("2");
        args.add("--clients");
        args.add("1");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();
        assertEquals(2, count(cli.deploymentPlan, WorkerType.MEMBER));
        assertEquals(1, count(cli.deploymentPlan, WorkerType.JAVA_CLIENT));
    }

    private int count(DeploymentPlan deploymentPlan, WorkerType type) {
        Map<SimulatorAddress, List<WorkerProcessSettings>> deployment = deploymentPlan.getWorkerDeployment();
        int result = 0;
        for (List<WorkerProcessSettings> list : deployment.values()) {
            for (WorkerProcessSettings settings : list) {
                if (settings.getWorkerType().equals(type)) {
                    result++;
                }
            }
        }
        return result;
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_propertiesFile() {
        args.add("--propertiesFile");
        args.add("not.found");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_tooMany() {
        args.add("test.properties");
        args.add("test.properties");

        createCoordinatorCli();
    }

    @Test(expected = CommandLineExitException.class)
    public void testInit_testSuite_notFound() {
        args.add("not.found");

        createCoordinatorCli();
    }

    @Test(expected = Exception.class)
    public void testInit_syncToTestPhase_invalid() {
        args.add("--syncToTestPhase");
        args.add("INVALID");
        args.add(testSuiteFile.getAbsolutePath());

        createCoordinatorCli();
    }

    @Test
    public void testInit_syncToTestPhase_default() {
        args.add(testSuiteFile.getAbsolutePath());
        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.getLastTestPhase(), cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_globalPrepare() {
        args.add("--syncToTestPhase");
        args.add("GLOBAL_PREPARE");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.GLOBAL_PREPARE, cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_syncToTestPhase_localVerify() {
        args.add("--syncToTestPhase");
        args.add("LOCAL_VERIFY");
        args.add(testSuiteFile.getAbsolutePath());

        CoordinatorCli cli = createCoordinatorCli();

        assertEquals(TestPhase.LOCAL_VERIFY, cli.coordinatorParameters.getLastTestPhaseToSync());
    }

    @Test
    public void testInit_memberConfigFileInWorkDir() {
        File memberConfigFile = new File("hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, memberConfigFile);

        args.add(testSuiteFile.getAbsolutePath());

        try {
            CoordinatorCli cli = createCoordinatorCli();
            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.MEMBER).getEnvironment().get("HAZELCAST_CONFIG"));
        } finally {
            deleteQuiet(memberConfigFile);
        }
    }

    @Test
    public void testInit_clientConfigFileInWorkDir() {
        File clientConfigFile = new File("client-hazelcast.xml").getAbsoluteFile();
        writeText(HAZELCAST_XML, clientConfigFile);

        args.add(testSuiteFile.getAbsolutePath());
        try {
            CoordinatorCli cli = createCoordinatorCli();
            assertEquals(HAZELCAST_XML, cli.workerParametersMap.get(WorkerType.JAVA_CLIENT).getEnvironment().get("HAZELCAST_CONFIG"));
        } finally {
            deleteQuiet(clientConfigFile);
        }
    }

    @Test
    public void testInit_withLocalSetup() {
        File simulatorProperties = new File(getUserDir(), "simulator.properties").getAbsoluteFile();
        writeText(format("%s=%s", PROPERTY_CLOUD_PROVIDER, CloudProviderUtils.PROVIDER_LOCAL), simulatorProperties);

        try {
            CoordinatorCli cli = createCoordinatorCli();

            ComponentRegistry componentRegistry = cli.componentRegistry;
            assertEquals(1, componentRegistry.agentCount());

            AgentData firstAgent = componentRegistry.getFirstAgent();
            assertEquals("localhost", firstAgent.getPublicAddress());
            assertEquals("localhost", firstAgent.getPrivateAddress());
        } finally {
            deleteQuiet(simulatorProperties);
        }
    }

    private CoordinatorCli createCoordinatorCli() {
        return new CoordinatorCli(getArgs());
    }

    private String[] getArgs() {
        return args.toArray(new String[0]);
    }
}
