package com.hazelcast.simulator.tests.external;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import java.io.File;

import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;

public class ExternalClientStarterTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientStarterTest.class);

    // properties
    public String binaryName = "binaryName";
    public String arguments = "";

    private final SimulatorProperties props = new SimulatorProperties();
    private final Bash bash = new Bash(props);

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        // delete the local binary, so it won't get downloaded
        deleteQuiet(new File(binaryName));
    }

    @Run
    public void run() {
        LOGGER.info("Starting external client: " + binaryName);
        bash.execute("../upload/" + binaryName + " " + arguments + " >> external-client.log &");
    }
}
