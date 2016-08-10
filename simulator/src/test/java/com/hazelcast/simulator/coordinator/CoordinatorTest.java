package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.TestSuite;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CoordinatorTest {

    private SimulatorProperties properties;
    private Coordinator coordinator;
    private TestSuite testSuite;

    @Before
    public void setUp() {
        setDistributionUserDir();

        testSuite = new TestSuite("testrun-" + System.currentTimeMillis());
        ComponentRegistry componentRegistry = new ComponentRegistry();

        properties = mock(SimulatorProperties.class);

        CoordinatorParameters coordinatorParameters = mock(CoordinatorParameters.class);
        when(coordinatorParameters.getSimulatorProperties()).thenReturn(properties);

        WorkerParameters workerParameters = mock(WorkerParameters.class);

        ClusterLayoutParameters clusterLayoutParameters = mock(ClusterLayoutParameters.class);

        ClusterLayout clusterLayout = mock(ClusterLayout.class);

        coordinator = new Coordinator(testSuite, componentRegistry, coordinatorParameters, workerParameters,
                clusterLayoutParameters, clusterLayout);
    }

    @After
    public void tearDown() {
        deleteQuiet(new File(testSuite.getId()).getAbsoluteFile());
        resetUserDir();
    }

    @Test
    public void testRun() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);
        when(properties.getVersionSpec()).thenReturn("outofthebox");

        coordinator.run();
    }
}
