package com.hazelcast.simulator.protocol.connector;

import com.hazelcast.simulator.protocol.core.ResponseFuture;
import com.hazelcast.simulator.protocol.core.SimulatorMessage;
import com.hazelcast.simulator.protocol.core.SimulatorProtocolException;
import com.hazelcast.simulator.protocol.operation.IntegrationTestOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.simulator.protocol.core.ResponseFuture.createFutureKey;
import static com.hazelcast.simulator.protocol.core.ResponseFuture.createInstance;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.COORDINATOR;
import static com.hazelcast.simulator.utils.CommonUtils.joinThread;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CommunicatorConnectorTest {

    private static final int DEFAULT_TIMEOUT = 30000;

    private CommunicatorConnector connector;

    private static final int COORDINATOR_PORT = 0;

    @Before
    public void setUp() {
        connector = new CommunicatorConnector("127.0.0.1", COORDINATOR_PORT);
    }

    @After
    public void tearDown() {
        connector.shutdown();
    }

    @Test(timeout = DEFAULT_TIMEOUT)
    public void testWrite_withInterruptedException() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        String futureKey = createFutureKey(COORDINATOR, 1, 1);
        ResponseFuture responseFuture = createInstance(connector.getFutureMap(), futureKey);

        ClientConnector coordinator = mock(ClientConnector.class);
        when(coordinator.writeAsync(any(SimulatorMessage.class))).thenReturn(responseFuture);

        connector.addCoordinator(coordinator);

        Thread thread = new Thread() {
            @Override
            public void run() {
                latch.countDown();
                try {
                    connector.write(new IntegrationTestOperation());
                } catch (SimulatorProtocolException e) {
                    exceptionThrown.set(true);
                }
            }
        };
        thread.start();

        latch.await();
        thread.interrupt();

        joinThread(thread);

        assertTrue("Expected SimulatorProtocolException to be thrown, but flag was not set", exceptionThrown.get());
    }
}
