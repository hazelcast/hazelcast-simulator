package com.hazelcast.stabilizer.communicator;

import com.hazelcast.stabilizer.common.AgentAddress;
import com.hazelcast.stabilizer.common.AgentsFile;
import com.hazelcast.stabilizer.common.messaging.Message;
import com.hazelcast.stabilizer.coordinator.remoting.AgentsClient;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.stabilizer.utils.CommonUtils.getVersion;
import static com.hazelcast.stabilizer.utils.FileUtils.getStablizerHome;
import static java.lang.String.format;

public class Communicator {
    private final static Logger log = Logger.getLogger(Communicator.class.getName());
    private final static String STABILIZER_HOME = getStablizerHome().getAbsolutePath();

    public File agentsFile;
    protected AgentsClient agentsClient;

    public Message message;

    public static void main(String[] args) throws IOException {
        log.info("Stabilizer Communicator");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        Communicator communicator = new Communicator();
        CommunicatorCli cli = new CommunicatorCli(communicator);
        cli.init(args);

        log.info(format("Loading agents file: %s", communicator.agentsFile.getAbsolutePath()));
        try {
            communicator.run();
            System.exit(0);
        } catch (Exception e) {
            log.error("Failed to communicate", e);
            System.exit(1);
        }
    }

    private void run() throws Exception {
        initAgents();
        agentsClient.sendMessage(message);
    }

    private void initAgents() throws Exception {
        List<AgentAddress> agentAddresses = AgentsFile.load(agentsFile);
        agentsClient = new AgentsClient(agentAddresses);
        agentsClient.start();
    }
}
