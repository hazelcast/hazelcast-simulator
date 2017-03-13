package com.hazelcast.simulator.worker;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.protocol.connector.WorkerConnector;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.common.WorkerType.MEMBER;
import static com.hazelcast.simulator.utils.FileUtils.appendText;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.HazelcastUtils.initClientHzConfig;
import static com.hazelcast.simulator.utils.HazelcastUtils.initMemberHzConfig;
import static org.junit.Assert.assertEquals;

public class WorkerTest {

    private File memberConfigFile;
    private File clientConfigFile;

    private static final String PUBLIC_ADDRESS = "127.0.0.1";
    private static final int AGENT_INDEX = 1;
    private static final int WORKER_INDEX = 1;
    private static final int WORKER_PORT = 9001;

    private Worker worker;

    @Before
    public void before() {
        setupFakeEnvironment();

        ComponentRegistry componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(PUBLIC_ADDRESS, PUBLIC_ADDRESS);

        SimulatorProperties properties =  new SimulatorProperties();
        properties.set("MANAGEMENT_CENTER_URL","none");

        String memberHzConfig = fileAsText(localResourceDirectory() + "/hazelcast.xml");
        memberHzConfig = initMemberHzConfig(memberHzConfig, componentRegistry, null, properties.asMap(), false);
        memberConfigFile = new File(getUserDir(), "hazelcast.xml");
        appendText(memberHzConfig, memberConfigFile);

        String clientHzConfig = fileAsText(localResourceDirectory() + "/client-hazelcast.xml");
        clientHzConfig = initClientHzConfig(clientHzConfig, componentRegistry, properties.asMap(), null);
        clientConfigFile = new File(getUserDir(), "client-hazelcast.xml");
        appendText(clientHzConfig, clientConfigFile);
    }

    @After
    public void after() throws Exception {
        if (worker != null) {
            worker.shutdown(false);
            worker.awaitShutdown();
        }

        Hazelcast.shutdownAll();
        tearDownFakeEnvironment();
    }

    @Test
    public void testConstructor_MemberWorker() throws Exception {
        worker = new Worker(
                MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, memberConfigFile.getAbsolutePath(), true, 10);
        worker.start();
        assertMemberWorker();
    }

    @Test
    public void testConstructor_ClientWorker() throws Exception {
        Hazelcast.newHazelcastInstance();

        worker = new Worker(
                WorkerType.JAVA_CLIENT, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, clientConfigFile.getAbsolutePath(), true, 10);
        worker.start();
        assertMemberWorker();
    }

    @Test
    public void testConstructor_noAutoCreateHzInstance() throws Exception {
        worker = new Worker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, "", false, 10);
        worker.start();
        assertMemberWorker();
    }

    @Test
    public void testConstructor_noAutoCreateHzInstance_withPerformanceMonitor() throws Exception {
        worker = new Worker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, "", false, 10);
        worker.start();
        assertMemberWorker();

    }

    @Test
    public void testConstructor_noAutoCreateHzInstance_withPerformanceMonitor_invalidInterval() throws Exception {
        worker = new Worker(MEMBER, PUBLIC_ADDRESS, AGENT_INDEX, WORKER_INDEX, WORKER_PORT, "", false, 0);
        worker.start();
        assertMemberWorker();
    }

    @Test
    public void testStartWorker() throws Exception {
        System.setProperty("workerId", "WorkerTest");
        System.setProperty("workerType", "MEMBER");
        System.setProperty("publicAddress", PUBLIC_ADDRESS);
        System.setProperty("agentIndex", String.valueOf(AGENT_INDEX));
        System.setProperty("workerIndex", String.valueOf(WORKER_INDEX));
        System.setProperty("workerPort", String.valueOf(WORKER_PORT));
        System.setProperty("hzConfigFile", memberConfigFile.getAbsolutePath());
        System.setProperty("autoCreateHzInstance", "true");
        System.setProperty("workerPerformanceMonitorIntervalSeconds", "10");

        worker = Worker.startWorker();
        assertMemberWorker();
    }

    private void assertMemberWorker() {
        assertEquals(PUBLIC_ADDRESS, worker.getPublicIpAddress());

        WorkerConnector workerConnector = worker.getWorkerConnector();
        assertEquals(WORKER_PORT, workerConnector.getPort());

        SimulatorAddress workerAddress = workerConnector.getAddress();
        assertEquals(AGENT_INDEX, workerAddress.getAgentIndex());
        assertEquals(WORKER_INDEX, workerAddress.getWorkerIndex());
    }
}
