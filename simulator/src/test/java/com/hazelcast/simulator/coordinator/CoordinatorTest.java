package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.cluster.ClusterLayout;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.test.TestSuite;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.resetUserDir;
import static com.hazelcast.simulator.TestEnvironmentUtils.setDistributionUserDir;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_EC2;
import static com.hazelcast.simulator.utils.CloudProviderUtils.PROVIDER_LOCAL;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.BRING_MY_OWN;
import static com.hazelcast.simulator.utils.jars.HazelcastJARs.OUT_OF_THE_BOX;
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
        when(properties.getHazelcastVersionSpec()).thenReturn(OUT_OF_THE_BOX);

        coordinator.run();
    }

    @Test
    public void testUploadFiles() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_EC2);

        coordinator.uploadFiles();
    }

    @Test
    public void testUploadFiles_whenLocalMode_thenReturn() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);
        when(properties.getHazelcastVersionSpec()).thenReturn(OUT_OF_THE_BOX);

        coordinator.uploadFiles();
    }

    @Test(expected = CommandLineExitException.class)
    public void testUploadFiles_whenLocalModeAndVersionSpec_thenThrowException() {
        when(properties.getCloudProvider()).thenReturn(PROVIDER_LOCAL);
        when(properties.getHazelcastVersionSpec()).thenReturn(BRING_MY_OWN);

        coordinator.uploadFiles();
    }
}
