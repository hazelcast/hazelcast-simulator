package com.hazelcast.simulator.communicator;

import com.hazelcast.simulator.common.messaging.Message;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.protocol.registry.ComponentRegistry;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.File;

import static com.hazelcast.simulator.utils.CommonUtils.exitWithError;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static com.hazelcast.simulator.utils.FileUtils.getSimulatorHome;
import static com.hazelcast.simulator.utils.SimulatorUtils.loadComponentRegister;
import static java.lang.String.format;

public final class Communicator {

    private static final Logger LOGGER = Logger.getLogger(Communicator.class);

    private final Message message;
    private final AgentsClient agentsClient;

    private Communicator(File agentsFile, Message message) {
        LOGGER.info(format("Loading agents file: %s", agentsFile.getAbsolutePath()));
        ComponentRegistry componentRegistry = loadComponentRegister(agentsFile);

        this.message = message;
        this.agentsClient = new AgentsClient(componentRegistry.getAgents());
    }

    static Communicator createInstance(File agentsFile, Message message) {
        return new Communicator(agentsFile, message);
    }

    private void run() {
        agentsClient.start();
        try {
            agentsClient.sendMessage(message);
            agentsClient.stop();

        } catch (Exception e) {
            throw new CommandLineExitException("Could not send message to agents", e);
        }

        LOGGER.info("Message sent!");
    }

    public static void main(String[] args) {
        try {
            LOGGER.info("Simulator Communicator");
            LOGGER.info(String.format("Version: %s", getSimulatorVersion()));
            LOGGER.info(format("SIMULATOR_HOME: %s", getSimulatorHome().getAbsolutePath()));

            Communicator communicator = CommunicatorCli.init(args);
            communicator.run();
        } catch (Exception e) {
            exitWithError(LOGGER, "Failed to communicate", e);
        }
    }
}
