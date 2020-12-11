package com.hazelcast.simulator.protocol;

import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.IpAndPort;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.operation.LogOperation;
import com.hazelcast.simulator.protocol.operation.SimulatorOperation;
import com.hazelcast.simulator.utils.AssertTask;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.JMSException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.SimulatorUtils.localIp;
import static com.hazelcast.simulator.utils.TestUtils.assertCompletesEventually;
import static com.hazelcast.simulator.utils.TestUtils.assertTrueEventually;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MessagingTest {

    private Broker broker;
    private Server agentServer;
    private CoordinatorClient client;
    private SimulatorAddress agentAddress = SimulatorAddress.fromString("A1");

    @Before
    public void before() {
        broker = new Broker();
        broker.start();
    }

    @After
    public void after() {
        closeQuietly(client);
        closeQuietly(agentServer);
        closeQuietly(broker);
    }

    @Test
    public void test() throws Exception {
        agentServer = new Server("agents")
                .setBrokerURL(broker.getBrokerURL())
                .setSelfAddress(agentAddress)
                .setProcessor(new OperationProcessor() {
                    @Override
                    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
                        System.out.println(op);
                        promise.answer("OK");
                    }
                })
                .start();

        client = new CoordinatorClient()
                .setProcessor(mock(OperationProcessor.class))
                .start()
                .connectToAgentBroker(agentAddress, new IpAndPort(localIp(), SimulatorProperties.DEFAULT_BROKER_PORT));

        final Future f = client.submit(agentAddress, new LogOperation("", Level.DEBUG));
        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() throws Exception {
                        assertTrue(f.isDone());
                        assertEquals("OK", f.get());
                    }
                }
        );
    }

    @Test
    public void testWhenAgentConnectionFails() throws Exception {
        final CountDownLatch received = new CountDownLatch(1);
        agentServer = new Server("agents")
                .setBrokerURL(broker.getBrokerURL())
                .setSelfAddress(agentAddress)
                .setProcessor(new OperationProcessor() {
                    @Override
                    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
                        // we don't do anything to let the future wait
                        received.countDown();
                    }
                })
                .start();

        client = new CoordinatorClient()
                .setProcessor(mock(OperationProcessor.class));
        client.getConnectionFactory().setMaxReconnectAttempts(1);
        client.start().connectToAgentBroker(agentAddress, new IpAndPort(localIp(), SimulatorProperties.DEFAULT_BROKER_PORT));

        final Future f = client.submit(agentAddress, new LogOperation("", Level.DEBUG));

        received.await();
        broker.close();

        assertCompletesEventually(f);
        try {
            f.get();
            fail();
        } catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof JMSException);
        }
    }

    @Test
    public void sendCoordinator() throws Exception {
        agentServer = new Server("agents")
                .setBrokerURL(broker.getBrokerURL())
                .setSelfAddress(agentAddress)
                .setProcessor(new OperationProcessor() {
                    @Override
                    public void process(SimulatorOperation op, SimulatorAddress source, Promise promise) throws Exception {
                    }
                })
                .start();

        final OperationProcessor clientOperationProcessor = mock(OperationProcessor.class);
        client = new CoordinatorClient()
                .setProcessor(clientOperationProcessor);
        client.getConnectionFactory().setMaxReconnectAttempts(1);
        client.start().connectToAgentBroker(agentAddress, new IpAndPort(localIp(), SimulatorProperties.DEFAULT_BROKER_PORT));

        agentServer.sendCoordinator(new LogOperation("Foo"));

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                verify(clientOperationProcessor).process(any(LogOperation.class), eq(agentAddress), any(Promise.class));
            }
        });
    }
}
