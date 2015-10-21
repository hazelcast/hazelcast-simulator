package com.hazelcast.simulator.tests.external;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.provisioner.Bash;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import java.io.File;
import java.util.UUID;

import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isClient;
import static com.hazelcast.simulator.tests.helpers.HazelcastTestUtils.isMemberNode;
import static com.hazelcast.simulator.utils.FileUtils.deleteQuiet;
import static com.hazelcast.simulator.utils.HostAddressPicker.pickHostAddress;
import static java.lang.String.format;

public class ExternalClientStarterTest {

    private static final ILogger LOGGER = Logger.getLogger(ExternalClientStarterTest.class);

    // properties
    public String binaryName = "binaryName";
    public String arguments = "";
    public String logFileName = "external-client";
    public int processCount = 1;

    private final SimulatorProperties props = new SimulatorProperties();
    private final Bash bash = new Bash(props);
    private final String ipAddress = pickHostAddress();

    private HazelcastInstance hazelcastInstance;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        hazelcastInstance = testContext.getTargetInstance();
        if (isClient(hazelcastInstance)) {
            hazelcastInstance.getAtomicLong("externalClientsStarted").addAndGet(processCount);
        }

        // delete the local binary, so it won't get downloaded again
        deleteQuiet(new File(binaryName));
    }

    @Run
    public void run() {
        if (isMemberNode(hazelcastInstance)) {
            return;
        }

        for (int i = 1; i <= processCount; i++) {
            String tmpArguments = arguments
                    .replace("$PROCESS_INDEX", String.valueOf(i))
                    .replace("$IP_ADDRESS", ipAddress)
                    .replace("$UUID", UUID.randomUUID().toString());

            String tmpLogFileName = logFileName + '_' + i;

            LOGGER.info(format("Starting external client: %s %s &>> %s.log", binaryName, tmpArguments, tmpLogFileName));
            bash.execute(format("../upload/%s %s &>> %s.log &", binaryName, tmpArguments, tmpLogFileName));
        }
    }
}
