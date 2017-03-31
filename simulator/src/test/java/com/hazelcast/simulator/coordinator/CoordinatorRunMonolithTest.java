package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.coordinator.registry.Registry;
import com.hazelcast.simulator.tests.FailingTest;
import com.hazelcast.simulator.tests.SuccessTest;
import com.hazelcast.simulator.vendors.HazelcastDriver;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.copy;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadSimulatorProperties;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class CoordinatorRunMonolithTest {

    private Registry registry;
    private Agent agent;
    private CoordinatorRunMonolith run;
    private Coordinator coordinator;
    private VendorDriver hazelcastDriver;

    @Before
    public void setUp() throws Exception {
        setupFakeEnvironment();

        File simulatorPropertiesFile = new File(getUserDir(), "simulator.properties");
        appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);

        SimulatorProperties simulatorProperties = loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties)
                .setSkipShutdownHook(true);

        agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 60);
        agent.start();

        registry = new Registry();
        registry.addAgent(localIp(), localIp());

        copy(new File(localResourceDirectory(), "hazelcast.xml"), new File(getUserDir(), "hazelcast.xml"));

        hazelcastDriver = new HazelcastDriver()
                .setAgents(registry.getAgents())
                .setAll(simulatorProperties.asPublicMap())
                .set("SESSION_ID", coordinatorParameters.getSessionId());

        coordinator = new Coordinator(registry, coordinatorParameters);
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

        DeploymentPlan deploymentPlan = new DeploymentPlan(hazelcastDriver, registry.getAgents())
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

        DeploymentPlan deploymentPlan = new DeploymentPlan(hazelcastDriver, registry.getAgents())
                .addToPlan(1, "member");

        run.init(deploymentPlan);

        boolean success = run.run(suite);

        assertFalse(success);
    }
}
