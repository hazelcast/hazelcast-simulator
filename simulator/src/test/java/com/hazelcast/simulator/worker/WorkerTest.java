package com.hazelcast.simulator.worker;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.registry.ComponentRegistry;
import com.hazelcast.simulator.protocol.Broker;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import com.hazelcast.simulator.utils.FileUtils;
import com.hazelcast.simulator.vendors.HazelcastDriver;
import com.hazelcast.simulator.vendors.VendorDriver;
import com.hazelcast.simulator.worker.operations.TerminateWorkerOperation;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.TestEnvironmentUtils.localResourceDirectory;
import static com.hazelcast.simulator.TestEnvironmentUtils.setupFakeEnvironment;
import static com.hazelcast.simulator.TestEnvironmentUtils.tearDownFakeEnvironment;
import static com.hazelcast.simulator.utils.CommonUtils.closeQuietly;
import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static org.junit.Assert.assertEquals;

public class WorkerTest {

    private static final String PUBLIC_ADDRESS = "127.0.0.1";
    private static final int AGENT_INDEX = 1;
    private static final int WORKER_INDEX = 1;

    private Worker worker;
    private static Broker broker;
    private SimulatorAddress workerAddress;
    private WorkerParameters parameters;

    @Before
    public void before() {
        setupFakeEnvironment();

        ComponentRegistry componentRegistry = new ComponentRegistry();
        componentRegistry.addAgent(PUBLIC_ADDRESS, PUBLIC_ADDRESS);

        SimulatorProperties properties = new SimulatorProperties()
                .set("MANAGEMENT_CENTER_URL", "none");

        VendorDriver driver = new HazelcastDriver()
                .setAgents(componentRegistry.getAgents())
                .setAll(properties.asPublicMap())
                .set("CONFIG", fileAsText(localResourceDirectory() + "/hazelcast.xml"));
        workerAddress = new SimulatorAddress(AddressLevel.WORKER, AGENT_INDEX, WORKER_INDEX, 0);

        parameters = driver.loadWorkerParameters("member")
                .set("WORKER_ADDRESS", workerAddress)
                .set("PUBLIC_ADDRESS", PUBLIC_ADDRESS);

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith("file:")) {
                FileUtils.writeText(entry.getValue(), new File(getUserDir(), key.substring(5, key.length())));
            }
        }
    }

    @BeforeClass
    public static void beforeClass() {
        broker = new Broker().start();
    }

    @AfterClass
    public static void afterClass() {
        closeQuietly(broker);
    }

    @After
    public void after() throws Exception {
        if (worker != null) {
            worker.shutdown(new TerminateWorkerOperation(false));
            worker.awaitShutdown();
        }

        Hazelcast.shutdownAll();
        tearDownFakeEnvironment();
    }

    @Test
    public void testStartWorker() throws Exception {
        worker = new Worker(parameters.asMap());
        worker.start();
        assertEquals(PUBLIC_ADDRESS, worker.getPublicIpAddress());
    }
}
