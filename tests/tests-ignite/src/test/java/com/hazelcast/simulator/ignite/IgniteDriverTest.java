package com.hazelcast.simulator.ignite;

import com.hazelcast.simulator.TestEnvironmentUtils;
import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.utils.SimulatorUtils;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.ignite.Ignite;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;

public class IgniteDriverTest {
    private AgentData agent;

    @BeforeClass
    public static void beforeClass() throws Exception {
        TestEnvironmentUtils.setupFakeEnvironment();
        TestEnvironmentUtils.createAgentsFileWithLocalhost();
    }

    @AfterClass
    public static void afterClass() {
        TestEnvironmentUtils.tearDownFakeEnvironment();
    }

    @Before
    public void before(){
        agent = new AgentData(1, SimulatorUtils.localIp(), SimulatorUtils.localIp());
    }

    @Test
    public void test() throws Exception {
         VendorDriver<Ignite> driverAtCoordinator = new IgniteDriver()
                .setAgents(asList(agent));

        WorkerParameters workerParameters = driverAtCoordinator.loadWorkerParameters("member",agent.getAddressIndex());
        for(Map.Entry<String,String> entry: workerParameters.entrySet()){
            String key = entry.getKey();
            if (key.startsWith("file:")) {
                writeText(entry.getValue(), new File(getUserDir(), key.substring(5, key.length())));
            }
        }

        VendorDriver<Ignite> driverAtWorker = new IgniteDriver()
                .setAll(workerParameters.asMap());

        driverAtWorker.createVendorInstance();
        Ignite ignite = driverAtWorker.getVendorInstance();
        assertNotNull(ignite);
        driverAtWorker.close();
    }
}
