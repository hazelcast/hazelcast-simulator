package com.hazelcast.simulator.worker;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.deleteLogs;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initClientHzConfig;
import static com.hazelcast.simulator.coordinator.WorkerParameters.initMemberHzConfig;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.worker.WorkerType.CLIENT;
import static com.hazelcast.simulator.worker.WorkerType.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MemberWorkerTest {

    private static final String MEMBER_CONFIG_FILE = "memberWorkerTest-hazelcast.xml";
    private static final String CLIENT_CONFIG_FILE = "memberWorkerTest-client-hazelcast.xml";

    private static final String PUBLIC_ADDRESS = "127.0.0.1";
    private static final int AGENT_INDEX = 1;
    private static final int WORKER_INDEX = 1;
    private static final int WORKER_PORT = 9001;

    private MemberWorker worker;

    @Before
    public void setUp() {
        ComponentRegistry componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(PUBLIC_ADDRESS, PUBLIC_ADDRESS);

        SimulatorProperties properties = mock(SimulatorProperties.class);
        when(properties.get("MANAGEMENT_CENTER_URL")).thenReturn("none");

        String memberHzConfig = fileAsText("simulator/src/test/resources/hazelcast.xml");
        memberHzConfig = initMemberHzConfig(memberHzConfig, componentRegistry, 5701, null, properties);
        appendText(memberHzConfig, MEMBER_CONFIG_FILE);

        String clientHzConfig = fileAsText("simulator/src/test/resources/client-hazelcast.xml");
        clientHzConfig = initClientHzConfig(clientHzConfig, componentRegistry, 5701, null);
        appendText(clientHzConfig, CLIENT_CONFIG_FILE);
    }

    @After
    public void tearDown() throws Exception {
        try {
            if (worker != null) {
                worker.shutdown();
                worker.awaitShutdown();
            }

            Hazelcast.shutdownAll();
        } finally {
            deleteLogs();

            deleteQuiet(new File("throughput.txt"));
            deleteQuiet(new File("worker.address"));

            deleteQuiet(new File(MEMBER_CONFIG_FILE));
            deleteQuiet(new File(CLIENT_CONFIG_FILE));
        }
    }

    @Test
    public void testConstructor_MemberWorker() throws Exception {
        worker = new MemberWorker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, true, 10, MEMBER_CONFIG_FILE);
        assertMemberWorker();
    }

    @Test
    public void testConstructor_ClientWorker() throws Exception {
        Hazelcast.newHazelcastInstance();

        worker = new MemberWorker(CLIENT, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, true, 10, CLIENT_CONFIG_FILE);
        assertMemberWorker();
    }

    @Test
    public void testConstructor_noAutoCreateHzInstance() throws Exception {
        worker = new MemberWorker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, false, 10, "");
        assertMemberWorker();
    }

    @Test
    public void testConstructor_noAutoCreateHzInstance_withPerformanceMonitor() throws Exception {
        worker = new MemberWorker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, false, 10, "");
        assertMemberWorker();

        worker.startPerformanceMonitor();
        worker.shutdownPerformanceMonitor();
    }

    @Test
    public void testConstructor_noAutoCreateHzInstance_withPerformanceMonitor_invalidInterval() throws Exception {
        worker = new MemberWorker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, false, 0, "");
        assertMemberWorker();

        worker.startPerformanceMonitor();
        worker.shutdownPerformanceMonitor();
    }

    @Test
    public void testStartWorker() throws Exception {
        System.setProperty("workerId", "MemberWorkerTest");
        System.setProperty("workerType", "MEMBER");
        System.setProperty("publicAddress", PUBLIC_ADDRESS);
        System.setProperty("agentIndex", String.valueOf(AGENT_INDEX));
        System.setProperty("workerIndex", String.valueOf(WORKER_INDEX));
        System.setProperty("workerPort", String.valueOf(WORKER_PORT));
        System.setProperty("hzConfigFile", MEMBER_CONFIG_FILE);
        System.setProperty("autoCreateHzInstance", "true");
        System.setProperty("workerPerformanceMonitorIntervalSeconds", "10");

        worker = MemberWorker.startWorker();
        assertMemberWorker();
    }

    private void assertMemberWorker() {
        WorkerConnector workerConnector = worker.getWorkerConnector();
        assertEquals(WORKER_PORT, workerConnector.getPort());

        SimulatorAddress workerAddress = workerConnector.getAddress();
        assertEquals(AGENT_INDEX, workerAddress.getAgentIndex());
        assertEquals(WORKER_INDEX, workerAddress.getWorkerIndex());
    }
}
