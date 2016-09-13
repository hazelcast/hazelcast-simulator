package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.agent.Agent;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
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

public class CoordinatorRunTest {

    private String hzConfig;
    private ComponentRegistry componentRegistry;
    private Agent agent;
    private AgentData agentData;
    private CoordinatorRun run;
    private WorkerParameters workerParameters;

    @Before
    public void before() {
        setupFakeEnvironment();

        File simulatorPropertiesFile = new File(getUserDir(), "simulator.properties");
        appendText("CLOUD_PROVIDER=embedded\n", simulatorPropertiesFile);
        SimulatorProperties simulatorProperties = loadSimulatorProperties();

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(simulatorProperties);

        this.agent = new Agent(1, "127.0.0.1", simulatorProperties.getAgentPort(), 10, 60);
        agent.start();
        agent.setSessionId(coordinatorParameters.getSessionId());
        this.componentRegistry = new ComponentRegistry();
        this.agentData = componentRegistry.addAgent("127.0.0.1", "127.0.0.1");

        this.hzConfig = fileAsText(new File(localResourceDirectory(), "hazelcast.xml"));
        initMemberHzConfig(hzConfig, componentRegistry, null, simulatorProperties, false);

        File scriptFile = new File(internalDistPath() + "/conf/worker-hazelcast-member.sh");
        File logFile = new File(internalDistPath() + "/conf/agent-log4j.xml");

        this.workerParameters = new WorkerParameters()
                .setVersionSpec(simulatorProperties.getVersionSpec())
                .addEnvironment(simulatorProperties.asMap())
                .addEnvironment("HAZELCAST_CONFIG", hzConfig)
                .addEnvironment("LOG4j_CONFIG", fileAsText(logFile))
                .addEnvironment("AUTOCREATE_HAZELCAST_INSTANCE", "true")
                .addEnvironment("JVM_OPTIONS", "")
                .addEnvironment("WORKER_PERFORMANCE_MONITOR_INTERVAL_SECONDS", "10")
                .setWorkerStartupTimeout(simulatorProperties.getAsInteger("WORKER_STARTUP_TIMEOUT_SECONDS"))
                .setWorkerScript(fileAsText(scriptFile));

        this.run = new CoordinatorRun(componentRegistry, coordinatorParameters);
    }

    @After
    public void after() throws InterruptedException {
        closeQuietly(run);
        agent.shutdown();
        tearDownFakeEnvironment();
    }

    @Test
    public void success() throws Exception {
        TestSuite suite = new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", SuccessTest.class));

        DeploymentPlan deploymentPlan = DeploymentPlan.createDeploymentPlan(
                componentRegistry,
                workerParameters,
                WorkerType.MEMBER,
                1,
                singletonList(new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0)));

        run.init(deploymentPlan);
        boolean success = this.run.run(suite);
        assertTrue(success);
    }

    @Test
    public void failing() throws Exception {
        TestSuite suite = new TestSuite()
                .setDurationSeconds(5)
                .addTest(new TestCase("foo")
                        .setProperty("threadCount", 1)
                        .setProperty("class", FailingTest.class));

        DeploymentPlan deploymentPlan = DeploymentPlan.createDeploymentPlan(
                componentRegistry,
                workerParameters,
                WorkerType.MEMBER,
                1,
                singletonList(new SimulatorAddress(AGENT, agent.getAddressIndex(), 0, 0)));

        run.init(deploymentPlan);
        boolean success = this.run.run(suite);
        assertFalse(success);
    }
}
