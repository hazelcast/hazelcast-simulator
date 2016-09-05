package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

    private SimulatorProperties properties;
    private Coordinator coordinator;
    private TestSuite testSuite;
    private DeploymentPlan deploymentPlan;

    @Before
    public void before() {
        setupFakeEnvironment();

        testSuite = new TestSuite();
        ComponentRegistry componentRegistry = new ComponentRegistry();

        properties = mock(SimulatorProperties.class);

        CoordinatorParameters coordinatorParameters = new CoordinatorParameters()
                .setSimulatorProperties(properties);

        deploymentPlan = new DeploymentPlan();

        coordinator = new Coordinator(componentRegistry, coordinatorParameters);
    }

    @After
    public void after() {
        tearDownFakeEnvironment();
    }

    // todo: this test tests nothing; it just triggers code to be touched.
    @Test
    public void testRun() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);
        when(properties.getVersionSpec()).thenReturn("outofthebox");

        coordinator.run(deploymentPlan, testSuite);
    }
}
