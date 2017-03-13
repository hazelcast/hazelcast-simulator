package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.internalDistPath;
import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.coordinator.DeploymentPlan.createDeploymentPlan;
import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static java.util.Collections.singletonList;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class CoordinatorRunMonolithTest {

    private ComponentRegistry componentRegistry;
    private Agent agent;
    private CoordinatorRunMonolith run;
    private WorkerParameters workerParameters;
    private Coordinator coordinator;

    @Before
    public void setUp() {
        setupFakeEnvironment();

        File simulatorPropertiesFile = new File(getUserDir(), "simulator.properties");
        appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);
        SimulatorProperties simulatorProperties = loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setSkipShutdownHook(true);

        agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 10, 60);
        agent.start();
        agent.setSessionId(coordinatorParameters.getSessionId());
        componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent("127.0.0.1", "127.0.0.1");

        String hzConfig = fileAsText(new File(localResourceDirectory(), "hazelcast.xml"));
        initMemberHzConfig(hzConfig, componentRegistry, null, simulatorProperties.asMap(), false);

        File scriptFile = new File(internalDistPath() + "/conf/worker.sh");
        File logFile = new File(internalDistPath() + "/conf/agent-log4j.xml");

        workerParameters = new WorkerParameters()
                .setVersionSpec(simulatorProperties.getVersionSpec())
                .addEnvironment(simulatorProperties.asMap())
                .addEnvironment("HAZELCAST_CONFIG", hzConfig)
                .addEnvironment("LOG4j_CONFIG", fileAsText(logFile))
                .addEnvironment("AUTOCREATE_HAZELCAST_INSTANCE", "true")
                .addEnvironment("JVM_OPTIONS", "")
                .addEnvironment("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "10")
                .addEnvironment("VENDOR", "hazelcast")
                .addEnvironment("WORKER_TYPE", "member")
                .setWorkerStartupTimeout(simulatorProperties.getWorkerStartupTimeoutSeconds())
                .setWorkerScript(fileAsText(scriptFile));

        coordinator = new Coordinator(componentRegistry, coordinatorParameters);
        coordinator.start();

        run = new CoordinatorRunMonolith(coordinator, coordinatorParameters);
    }

    @After
    public void tearDown() throws InterruptedException {
        closeQuietly(coordinator);
        closeQuietly(agent);
        tearDownFakeEnvironment();
    }

    @Test
    public void success() throws Exception {
        TestSuite suite = new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", SuccessTest.class));

        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry,
                workerParameters,
                WorkerType.MEMBER,
                1,
                singletonList(new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0)));

        run.init(deploymentPlan);

        boolean success = run.run(suite);

        assertTrue(success);
    }

    @Test
    public void failing() throws Exception {
        TestSuite suite = new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", FailingTest.class));

        DeploymentPlan deploymentPlan = createDeploymentPlan(
                componentRegistry,
                workerParameters,
                WorkerType.MEMBER,
                1,
                singletonList(new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0)));

        run.init(deploymentPlan);

        boolean success = run.run(suite);

        assertFalse(success);
    }
}
