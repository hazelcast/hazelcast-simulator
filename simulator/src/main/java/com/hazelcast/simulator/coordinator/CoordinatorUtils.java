package com.hazelcast.simulator.coordinator;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import com.hazelcast.simulator.worker.WorkerType;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.String.format;

final class CoordinatorUtils {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorUtils.class);

    private CoordinatorUtils() {
    }

    static String createAddressConfig(String tagName, List<String> addresses, CoordinatorParameters parameters) {
        StringBuilder members = new StringBuilder();
        for (String hostAddress : addresses) {
            members.append("<").append(tagName).append(">")
                    .append(hostAddress)
                    .append(":").append(getPort(parameters))
                    .append("</").append(tagName).append(">\n");
        }
        return members.toString();
    }

    private static int getPort(CoordinatorParameters parameters) {
        try {
            byte[] configString = parameters.getMemberHzConfig().getBytes("UTF-8");
            Config config = new XmlConfigBuilder(new ByteArrayInputStream(configString)).build();
            return config.getNetworkConfig().getPort();
        } catch (UnsupportedEncodingException e) {
            throw new CommandLineExitException("Could not get port from settings", e);
        }
    }

    static int getMaxTestCaseIdLength(List<TestCase> testCaseList) {
        int maxLength = Integer.MIN_VALUE;
        for (TestCase testCase : testCaseList) {
            String testCaseId = testCase.getId();
            if (testCaseId != null && !testCaseId.isEmpty() && testCaseId.length() > maxLength) {
                maxLength = testCaseId.length();
            }
        }
        return (maxLength > 0) ? maxLength : 0;
    }

    static List<AgentMemberLayout> initMemberLayout(AgentsClient agentsClient, CoordinatorParameters parameters) {
        int agentCount = agentsClient.getAgentCount();
        int dedicatedMemberMachineCount = parameters.getDedicatedMemberMachineCount();
        int memberWorkerCount = parameters.getMemberWorkerCount();
        int clientWorkerCount = parameters.getClientWorkerCount();

        if (dedicatedMemberMachineCount > agentCount) {
            throw new CommandLineExitException(format("dedicatedMemberMachineCount %d can't be larger than number of agents %d",
                    dedicatedMemberMachineCount, agentCount));
        }
        if (clientWorkerCount > 0 && agentCount - dedicatedMemberMachineCount < 1) {
            throw new CommandLineExitException("dedicatedMemberMachineCount is too big, there are no machines left for clients!");
        }

        List<AgentMemberLayout> agentMemberLayouts = initAgentMemberLayouts(agentsClient);

        assignDedicatedMemberMachines(agentCount, agentMemberLayouts, dedicatedMemberMachineCount);

        AtomicInteger currentIndex = new AtomicInteger(0);
        for (int i = 0; i < memberWorkerCount; i++) {
            // assign server nodes
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.CLIENT);
            agentLayout.addWorker(WorkerType.MEMBER, parameters);
        }
        for (int i = 0; i < clientWorkerCount; i++) {
            // assign the clients
            AgentMemberLayout agentLayout = findNextAgentLayout(currentIndex, agentMemberLayouts, AgentMemberMode.MEMBER);
            agentLayout.addWorker(WorkerType.CLIENT, parameters);
        }

        // log the layout
        for (AgentMemberLayout agentMemberLayout : agentMemberLayouts) {
            LOGGER.info(format("    Agent %s members: %d clients: %d mode: %s",
                    agentMemberLayout.getPublicAddress(),
                    agentMemberLayout.getCount(WorkerType.MEMBER),
                    agentMemberLayout.getCount(WorkerType.CLIENT),
                    agentMemberLayout.getAgentMemberMode()
            ));
        }

        return agentMemberLayouts;
    }

    private static List<AgentMemberLayout> initAgentMemberLayouts(AgentsClient agentsClient) {
        List<AgentMemberLayout> agentMemberLayouts = new LinkedList<AgentMemberLayout>();
        for (String agentIp : agentsClient.getPublicAddresses()) {
            AgentMemberLayout layout = new AgentMemberLayout(agentIp, AgentMemberMode.MIXED);
            agentMemberLayouts.add(layout);
        }
        return agentMemberLayouts;
    }

    private  static void assignDedicatedMemberMachines(int agentCount, List<AgentMemberLayout> agentMemberLayouts,
                                              int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentMemberMode(agentMemberLayouts, 0, dedicatedMemberMachineCount, AgentMemberMode.MEMBER);
            assignAgentMemberMode(agentMemberLayouts, dedicatedMemberMachineCount, agentCount, AgentMemberMode.CLIENT);
        }
    }

    private static void assignAgentMemberMode(List<AgentMemberLayout> agentMemberLayouts, int startIndex, int endIndex,
                                              AgentMemberMode agentMemberMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentMemberLayouts.get(i).setAgentMemberMode(agentMemberMode);
        }
    }

    static AgentMemberLayout findNextAgentLayout(AtomicInteger currentIndex, List<AgentMemberLayout> agentMemberLayouts,
                                                 AgentMemberMode excludedAgentMemberMode) {
        int size = agentMemberLayouts.size();
        while (true) {
            AgentMemberLayout agentLayout = agentMemberLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.getAgentMemberMode() != excludedAgentMemberMode) {
                return agentLayout;
            }
        }
    }
}
