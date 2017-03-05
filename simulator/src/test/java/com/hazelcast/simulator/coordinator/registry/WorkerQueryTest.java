package com.hazelcast.simulator.coordinator.registry;

import com.hazelcast.simulator.agent.workerprocess.WorkerProcessSettings;
import com.hazelcast.simulator.common.WorkerType;
import com.hazelcast.simulator.coordinator.TargetType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.TestSupport.toMap;
import static com.hazelcast.simulator.common.WorkerType.JAVA_CLIENT;
import static com.hazelcast.simulator.common.WorkerType.LITE_MEMBER;
import static com.hazelcast.simulator.common.WorkerType.MEMBER;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class WorkerQueryTest {

    private LinkedList<WorkerData> list;
    private SimulatorAddress agent1;
    private SimulatorAddress agent2;

    @Before
    public void before() {
        list = new LinkedList<WorkerData>();
        agent1 = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        agent2 = new SimulatorAddress(AddressLevel.AGENT, 2, 0, 0);
    }

    @Test
    public void noFilters() {
        list.add(new WorkerData(agent1, newSettings(1, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery().execute(list);
        assertEquals(list, result);
    }

    @Test
    public void versionSpec() {
        list.add(new WorkerData(agent1, newSettings(1, MEMBER, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setVersionSpec("maven=3.7")
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void workerType() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setWorkerType(MEMBER.name())
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void maxCount() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        WorkerQuery query = new WorkerQuery()
                .setMaxCount(2);
        List<WorkerData> result = query
                .execute(list);
        assertEquals(asList(list.get(0), list.get(1)), result);
    }

    @Test
    public void workerTags() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6"), toMap("a", "10")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7"), toMap("a", "20")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8"), toMap("a", "10","b","20")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        WorkerQuery query = new WorkerQuery().setWorkerTags(toMap("a","10"));
        List<WorkerData> result = query
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void targetType_whenAll() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.ALL)
                .execute(list);
        assertEquals(list, result);
    }

    @Test
    public void targetType_whenPreferClients() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.CLIENT)
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void targetType_whenMember() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent1, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent1, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.MEMBER)
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void agentAddresses() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent2, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent2, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setAgentAddresses(singletonList(agent1.toString()))
                .execute(list);

        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void workerAddresses() {
        list.add(new WorkerData(agent1, newSettings(1, JAVA_CLIENT, "maven=3.6")));
        list.add(new WorkerData(agent2, newSettings(2, MEMBER, "maven=3.7")));
        list.add(new WorkerData(agent1, newSettings(3, LITE_MEMBER, "maven=3.8")));
        list.add(new WorkerData(agent2, newSettings(4, MEMBER, "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setWorkerAddresses(asList(list.get(0).getAddress().toString(), list.get(2).getAddress().toString()))
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    private WorkerProcessSettings newSettings(int workerIndex, WorkerType workerType, String versionSpec) {
        return new WorkerProcessSettings(workerIndex, workerType, versionSpec, "", 0, new HashMap<String, String>());
    }
}
