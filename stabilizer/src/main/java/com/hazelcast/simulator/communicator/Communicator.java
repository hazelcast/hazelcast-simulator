package com.hazelcast.simulator.communicator;

import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.utils.CommonUtils;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public class Communicator {
    private static final Logger log = Logger.getLogger(Communicator.class);
    private static final String SIMULATOR_HOME = getSimulatorHome().getAbsolutePath();

    public File agentsFile;
    protected AgentsClient agentsClient;

    public Message message;

    public static void main(String[] args) throws IOException {
        log.info("Simulator Communicator");
        log.info(String.format("Version: %s", CommonUtils.getSimulatorVersion()));
        log.info(format("SIMULATOR_HOME: %s", SIMULATOR_HOME));

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
