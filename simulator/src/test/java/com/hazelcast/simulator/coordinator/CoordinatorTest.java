package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.JavaProfiler;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestSuite;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

    private SimulatorProperties properties;
    private Coordinator coordinator;

    @Before
    public void setUp() {
        setDistributionUserDir();

        TestSuite testSuite = new TestSuite();
        ComponentRegistry componentRegistry = new ComponentRegistry();

        properties = mock(SimulatorProperties.class);

        CoordinatorParameters coordinatorParameters = mock(CoordinatorParameters.class);
        when(coordinatorParameters.getSimulatorProperties()).thenReturn(properties);

        WorkerParameters workerParameters = mock(WorkerParameters.class);
        when(workerParameters.getProfiler()).thenReturn(JavaProfiler.NONE);

        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);

        ClusterLayout clusterLayout = mock(ClusterLayout.class);

        coordinator = new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters,
                clusterLayoutParameters, clusterLayout);
    }

    @After
    public void tearDown() {
        resetUserDir();
    }

    @Test
    public void testRun() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);

        coordinator.run();
    }

    @Test
    public void testUploadFiles() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_EC2);

        coordinator.uploadFiles();
    }
}
