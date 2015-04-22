package com.hazelcast.simulator.communicator;

import com.hazelcast.simulator.common.AgentAddress;
import com.hazelcast.simulator.common.AgentsFile;
import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.List;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static java.lang.String.format;

public final class Communicator {

    private static final Logger LOGGER = Logger.getLogger(Communicator.class);

    File agentsFile;
    Message message;

    private AgentsClient agentsClient;

    private void run() {
        initAgents();

        try {
            agentsClient.sendMessage(message);
            agentsClient.stop();
        } catch (Exception e) {
            throw new CommandLineExitException("Could not send message to agents", e);
        }

        LOGGER.info("Message sent!");
    }

    private void initAgents() {
        List<AgentAddress> agentAddresses = AgentsFile.load(agentsFile);
        agentsClient = new AgentsClient(agentAddresses);
        agentsClient.start();
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Simulator Communicator");
            LOGGER.info(String.format("Version: %s", getSimulatorVersion()));
            LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

            Communicator communicator = new Communicator();
            CommunicatorCli cli = new CommunicatorCli(communicator, args);
            cli.init();

            LOGGER.info(format("Loading agents file: %s", communicator.agentsFile.getAbsolutePath()));

            communicator.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to communicate", e);
        }
    }
}
