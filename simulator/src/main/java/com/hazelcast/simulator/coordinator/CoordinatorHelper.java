package com.hazelcast.simulator.coordinator;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.simulator.agent.workerjvm.WorkerJvmSettings;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.coordinator.remoting.AgentsClient;
import com.hazelcast.simulator.test.TestCase;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.UnsupportedEncodingException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isEC2;
import static com.hazelcast.simulator.utils.CommonUtils.getSimulatorVersion;
import static java.lang.String.format;

final class CoordinatorHelper {

    private static final Logger LOGGER = Logger.getLogger(CoordinatorHelper.class);

    private CoordinatorHelper() {
    }

    static String createAddressConfig(String tagName, List<String> addresses, WorkerJvmSettings settings) {
        StringBuilder members = new StringBuilder();
        for (String hostAddress : addresses) {
            members.append("<").append(tagName).append(">")
                    .append(hostAddress)
                    .append(":").append(getPort(settings))
                    .append("</").append(tagName).append(">\n");
        }
        return members.toString();
    }

    private static int getPort(WorkerJvmSettings settings) {
        try {
            Config config = new XmlConfigBuilder(new ByteArrayInputStream(settings.hzConfig.getBytes("UTF-8"))).build();
            return config.getNetworkConfig().getPort();
        } catch (UnsupportedEncodingException e) {
            throw new CommandLineExitException("Could not get port from settings", e);
        }
    }

    static List<AgentMemberLayout> initAgentMemberLayouts(AgentsClient agentsClient, WorkerJvmSettings workerJvmSettings) {
        List<AgentMemberLayout> agentMemberLayouts = new LinkedList<AgentMemberLayout>();
        for (String agentIp : agentsClient.getPublicAddresses()) {
            AgentMemberLayout layout = new AgentMemberLayout(workerJvmSettings);
            layout.publicIp = agentIp;
            layout.agentMemberMode = AgentMemberMode.MIXED;
            agentMemberLayouts.add(layout);
        }
        return agentMemberLayouts;
    }

    static void assignDedicatedMemberMachines(int agentCount, List<AgentMemberLayout> agentMemberLayouts,
                                              int dedicatedMemberMachineCount) {
        if (dedicatedMemberMachineCount > 0) {
            assignAgentMemberMode(agentMemberLayouts, 0, dedicatedMemberMachineCount, AgentMemberMode.MEMBER);
            assignAgentMemberMode(agentMemberLayouts, dedicatedMemberMachineCount, agentCount, AgentMemberMode.CLIENT);
        }
    }

    private static void assignAgentMemberMode(List<AgentMemberLayout> agentMemberLayouts, int startIndex, int endIndex,
                                              AgentMemberMode agentMemberMode) {
        for (int i = startIndex; i < endIndex; i++) {
            agentMemberLayouts.get(i).agentMemberMode = agentMemberMode;
        }
    }

    static AgentMemberLayout findNextAgentLayout(AtomicInteger currentIndex, List<AgentMemberLayout> agentMemberLayouts,
                                                 AgentMemberMode excludedAgentMemberMode) {
        int size = agentMemberLayouts.size();
        while (true) {
            AgentMemberLayout agentLayout = agentMemberLayouts.get(currentIndex.getAndIncrement() % size);
            if (agentLayout.agentMemberMode != excludedAgentMemberMode) {
                return agentLayout;
            }
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

    static String getStartHarakiriMonitorCommand(SimulatorProperties props) {
        if (!isEC2(props.get("CLOUD_PROVIDER")) || !"true".equals(props.get("HARAKIRI_MONITOR_ENABLED"))) {
            LOGGER.info("HarakiriMonitor is not enabled or not running on EC2");
            return null;
        }
        String waitSeconds = props.get("HARAKIRI_MONITOR_WAIT_SECONDS");
        LOGGER.info(format("HarakiriMonitor will kill the EC2 instances in %s seconds", waitSeconds));
        return format(
                "nohup hazelcast-simulator-%s/bin/harakiri-monitor --cloudProvider %s --cloudIdentity %s --cloudCredential %s"
                        + " --waitSeconds %s > harakiri.out 2> harakiri.err < /dev/null &",
                getSimulatorVersion(),
                props.get("CLOUD_PROVIDER"),
                props.get("CLOUD_IDENTITY"),
                props.get("CLOUD_CREDENTIAL"),
                waitSeconds);
    }
}
