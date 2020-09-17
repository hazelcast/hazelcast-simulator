package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.TestEnvironmentUtils;
import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.utils.CommonUtils;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.utils.SimulatorUtils;
import com.hazelcast.simulator.hazelcast4.Hazelcast4Driver;
import com.hazelcast.simulator.drivers.Driver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class CoordinatorRunMonolithTest {

    private Registry registry;
    private Agent agent;
    private CoordinatorRunMonolith run;
    private Coordinator coordinator;
    private Driver driver;

    @Before
    public void setUp() throws Exception {
        TestEnvironmentUtils.setupFakeEnvironment();

        File simulatorPropertiesFile = new File(FileUtils.getUserDir(), "simulator.properties");
        FileUtils.appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);

        SimulatorProperties simulatorProperties = SimulatorUtils.loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setSkipShutdownHook(true);

        agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 60, null);
        agent.start();

        registry = new Registry();
        registry.addAgent(SimulatorUtils.localIp(), SimulatorUtils.localIp());

        FileUtils.copy(new File(TestEnvironmentUtils.localResourceDirectory(), "hazelcast.xml"), new File(FileUtils.getUserDir(), "hazelcast.xml"));

        driver = new Hazelcast4Driver()
                .setAgents(registry.getAgents())
                .setAll(simulatorProperties.asPublicMap())
                .set("SESSION_ID", coordinatorParameters.getSessionId());

        coordinator = new Coordinator(registry, coordinatorParameters);
        coordinator.start();

        run = new CoordinatorRunMonolith(coordinator, coordinatorParameters);
    }

    @After
    public void tearDown() {
        CommonUtils.closeQuietly(coordinator);
        CommonUtils.closeQuietly(agent);
        TestEnvironmentUtils.tearDownFakeEnvironment();
    }

    @Test
    public void success() throws Exception {
        TestSuite suite = new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", SuccessTest.class));

        DeploymentPlan deploymentPlan = new DeploymentPlan(driver, registry.getAgents())
                .addToPlan(1, "member");
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

        DeploymentPlan deploymentPlan = new DeploymentPlan(driver, registry.getAgents())
                .addToPlan(1, "member");

        run.init(deploymentPlan);

        boolean success = run.run(suite);

        assertFalse(success);
    }
}
