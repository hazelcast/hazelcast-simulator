package com.hazelcast.simulator.coordinator.remoting;

import com.hazelcast.simulator.protocol.registry.AgentData;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static com.hazelcast.simulator.utils.ExecutorFactory.createFixedThreadPool;
import static java.lang.String.format;

public class AgentsClient {

    private static final int EXECUTOR_TERMINATION_TIMEOUT_SECONDS = 10;
    private static final int AGENT_KEEP_ALIVE_INTERVAL_SECONDS = 60;

    private static final int WAIT_FOR_AGENT_START_TIMEOUT_SECONDS = 60;
    private static final int WAIT_FOR_AGENT_START_INTERVAL_SECONDS = 5;

    private static final Logger LOGGER = Logger.getLogger(AgentsClient.class);

    private final ExecutorService agentExecutor = createFixedThreadPool(100, AgentsClient.class);
    private final List<AgentClient> agents = new LinkedList<AgentClient>();

    private final PokeThread pokeThread = new PokeThread();

    public AgentsClient(List<AgentData> agentDataList) {
        for (AgentData agentData : agentDataList) {
            AgentClient client = new AgentClient(agentData);
            agents.add(client);
        }
    }

    public void start() {
        awaitAgentsReachable();

        // starts a poke thread which will repeatedly poke the agents to make sure they are not going to terminate themselves
        pokeThread.start();
    }

    private void awaitAgentsReachable() {
        LOGGER.info("--------------------------------------------------------------");
        LOGGER.info("Waiting for agents to start");
        LOGGER.info("--------------------------------------------------------------");

        List<AgentClient> uncheckedAgents = new LinkedList<AgentClient>(agents);
        int maxRetries = WAIT_FOR_AGENT_START_TIMEOUT_SECONDS / WAIT_FOR_AGENT_START_INTERVAL_SECONDS;
        for (int i = 0; i < maxRetries; i++) {
            Iterator<AgentClient> agentIterator = uncheckedAgents.iterator();
            while (agentIterator.hasNext()) {
                AgentClient agent = agentIterator.next();
                try {
                    agent.execute();
                    agentIterator.remove();
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " OK");
                } catch (Exception e) {
                    LOGGER.info("Connect to agent " + agent.getPublicAddress() + " FAILED");
                    LOGGER.debug(e);
                }
            }

            if (uncheckedAgents.isEmpty()) {
                break;
            }
            LOGGER.info(format("Sleeping %d seconds and retrying unchecked agents", WAIT_FOR_AGENT_START_INTERVAL_SECONDS));
            sleepSeconds(WAIT_FOR_AGENT_START_INTERVAL_SECONDS);
        }

        if (!uncheckedAgents.isEmpty()) {
            StringBuilder sb = new StringBuilder("The Coordinator has dropped these agents because they are not reachable:\n");
            for (AgentClient agent : uncheckedAgents) {
                sb.append("\t").append(agent.getPublicAddress()).append("\n");
            }

            LOGGER.warn("--------------------------------------------------------------");
            LOGGER.warn(sb.toString());
            LOGGER.warn("--------------------------------------------------------------");

            agents.removeAll(uncheckedAgents);
            if (agents.isEmpty()) {
                throw new CommandLineExitException("There are no reachable agents");
            }
        } else {
            LOGGER.info("--------------------------------------------------------------");
            LOGGER.info("All agents are reachable!");
            LOGGER.info("--------------------------------------------------------------");
        }
    }

    public void shutdown() throws Exception {
        pokeThread.running = false;
        pokeThread.interrupt();
        pokeThread.join();

        agentExecutor.shutdown();
        agentExecutor.awaitTermination(EXECUTOR_TERMINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    private class PokeThread extends Thread {

        private volatile boolean running = true;

        public void run() {
            while (running) {
                sleepSeconds(AGENT_KEEP_ALIVE_INTERVAL_SECONDS);
                if (running) {
                    executeOnAllWorkers();
                }
            }
        }

        private void executeOnAllWorkers(final Object... args) {
            ThreadSpawner spawner = new ThreadSpawner("AgentsClientExecuteOnAllWorkers", true);
            for (final AgentClient agentClient : agents) {
                spawner.spawn(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            agentClient.execute(args);
                        } catch (Exception e) {
                            throw new CommandLineExitException("Could not execute on " + agentClient.getPublicAddress(), e);
                        }
                    }
                });
            }
            spawner.awaitCompletion();
        }
    }
}
