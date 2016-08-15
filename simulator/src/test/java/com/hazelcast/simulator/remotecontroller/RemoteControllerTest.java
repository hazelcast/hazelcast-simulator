package com.hazelcast.simulator.remotecontroller;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.connector.RemoteControllerConnector;
import com.hazelcast.simulator.protocol.core.Response;
import com.hazelcast.simulator.protocol.operation.RemoteControllerOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.protocol.core.ResponseType.EXCEPTION_DURING_OPERATION_EXECUTION;
import static com.hazelcast.simulator.protocol.core.ResponseType.SUCCESS;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.REMOTE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class RemoteControllerTest {

    private SimulatorProperties properties;
    private RemoteControllerConnector connector;
    private RemoteController remoteController;

    @Before
    public void setUp() {
        setupFakeEnvironment();

        properties = mock(SimulatorProperties.class);
        when(properties.getCoordinatorPort()).thenReturn(5555);

        Response response = new Response(1, REMOTE, COORDINATOR, SUCCESS);

        connector = mock(RemoteControllerConnector.class);
        when(connector.write(any(SimulatorOperation.class))).thenReturn(response);

        remoteController = new RemoteController(properties, false);
        remoteController.setRemoteControllerConnector(connector);
    }

    @After
    public void tearDown() {
        tearDownFakeEnvironment();
    }

    @Test
    public void testStart() {
        remoteController.start();

        verify(connector).start();
        verifyNoMoreInteractions(connector);
    }

    @Test
    public void testShutdown() {
        remoteController.shutdown();

        verify(connector).shutdown();
        verifyNoMoreInteractions(connector);
    }

    @Test
    public void testListComponents() {
        remoteController.listComponents();
    }

    @Test(expected = CommandLineExitException.class)
    public void testSendOperation_withFailureResponse() {
        Response failureResponse = new Response(1, REMOTE, COORDINATOR, EXCEPTION_DURING_OPERATION_EXECUTION);
        when(connector.write(any(SimulatorOperation.class))).thenReturn(failureResponse);

        remoteController.sendOperation(RemoteControllerOperation.Type.LIST_COMPONENTS);
    }

    @Test
    public void testReport() {
        remoteController.log("test");
    }

    @Test
    public void testReport_whenIsQuiet() {
        remoteController = new RemoteController(properties, true);

        remoteController.log("test");
    }
}
