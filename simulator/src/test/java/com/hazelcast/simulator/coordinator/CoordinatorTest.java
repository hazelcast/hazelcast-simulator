package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.TestEnvironmentUtils;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.coordinator.deployment.DeploymentPlan;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.coordinator.deployment.DeploymentPlan.createEmptyDeploymentPlan;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

    private String sessionId = "CoordinatorTest-" + System.currentTimeMillis();
    private SimulatorProperties properties;
    private Coordinator coordinator;
    private TestSuite testSuite;

    @Before
    public void setUp() {
        setupFakeEnvironment();

        testSuite = new TestSuite();
        ComponentRegistry componentRegistry = new ComponentRegistry();

        properties = mock(SimulatorProperties.class);

        CoordinatorParameters coordinatorParameters = mock(CoordinatorParameters.class);
        when(coordinatorParameters.getSessionId()).thenReturn(sessionId);
        when(coordinatorParameters.getSimulatorProperties()).thenReturn(properties);

        WorkerParameters workerParameters = mock(WorkerParameters.class);

        DeploymentPlan deploymentPlan = createEmptyDeploymentPlan();

        coordinator = new Coordinator(componentRegistry, coordinatorParameters, workerParameters, deploymentPlan);
    }

    @After
    public void tearDown() {
        deleteQuiet(new File(sessionId).getAbsoluteFile());

        TestEnvironmentUtils.tearDownFakeEnvironment();
    }

    // todo: this test tests nothing; it just triggers code to be touched.
    @Test
    public void testRun() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);
        when(properties.getVersionSpec()).thenReturn("outofthebox");

        coordinator.run(testSuite);
    }
}
