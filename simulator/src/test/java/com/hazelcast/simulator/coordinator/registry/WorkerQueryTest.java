package com.hazelcast.simulator.coordinator.registry;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.TargetType;
import com.hazelcast.simulator.protocol.core.AddressLevel;
import com.hazelcast.simulator.protocol.core.SimulatorAddress;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.TestSupport.toMap;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
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
        agent1 = new SimulatorAddress(AddressLevel.AGENT, 1, 0);
        agent2 = new SimulatorAddress(AddressLevel.AGENT, 2, 0);
    }

    @Test
    public void noFilters() {
        list.add(new WorkerData(newParameters(agent1, 1, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery().execute(list);
        assertEquals(list, result);
    }

    @Test
    public void versionSpec() {
        list.add(new WorkerData(newParameters(agent1, 1, "member", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "member", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setVersionSpec("maven=3.7")
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void workerType() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setWorkerType("member")
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void maxCount() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        WorkerQuery query = new WorkerQuery()
                .setMaxCount(2);
        List<WorkerData> result = query
                .execute(list);
        assertEquals(asList(list.get(0), list.get(1)), result);
    }

    @Test
    public void workerTags() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6"), toMap("a", "10")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7"), toMap("a", "20")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8"), toMap("a", "10", "b", "20")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        WorkerQuery query = new WorkerQuery().setWorkerTags(toMap("a", "10"));
        List<WorkerData> result = query
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void targetType_whenAll() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.ALL)
                .execute(list);
        assertEquals(list, result);
    }

    @Test
    public void targetType_whenPreferClients() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.CLIENT)
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void targetType_whenMember() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent1, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent1, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setTargetType(TargetType.MEMBER)
                .execute(list);
        assertEquals(asList(list.get(1), list.get(3)), result);
    }

    @Test
    public void agentAddresses() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent2, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent2, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setAgentAddresses(singletonList(agent1.toString()))
                .execute(list);

        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    @Test
    public void workerAddresses() {
        list.add(new WorkerData(newParameters(agent1, 1, "javaclient", "maven=3.6")));
        list.add(new WorkerData(newParameters(agent2, 2, "member", "maven=3.7")));
        list.add(new WorkerData(newParameters(agent1, 3, "litemember", "maven=3.8")));
        list.add(new WorkerData(newParameters(agent2, 4, "member", "maven=3.7")));

        List<WorkerData> result = new WorkerQuery()
                .setWorkerAddresses(asList(list.get(0).getAddress().toString(), list.get(2).getAddress().toString()))
                .execute(list);
        assertEquals(asList(list.get(0), list.get(2)), result);
    }

    private WorkerParameters newParameters(SimulatorAddress agent, int workerIndex, String workerType, String versionSpec) {
        return new WorkerParameters()
                .set("WORKER_ADDRESS", new SimulatorAddress(WORKER, agent.getAgentIndex(), workerIndex))
                .set("WORKER_TYPE", workerType)
                .set("VERSION_SPEC", versionSpec);
    }
}
