package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.agentAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.coordinatorAddress;
import static com.hazelcast.simulator.protocol.core.SimulatorAddress.workerAddress;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SimulatorAddressTest {

    @Test
    public void testGetAddressLevel() {
        assertEquals(COORDINATOR, coordinatorAddress().getAddressLevel());
        assertEquals(AGENT, agentAddress(1).getAddressLevel());
        assertEquals(WORKER, workerAddress(1, 1).getAddressLevel());
    }

    @Test
    public void testGetAgentIndex() {
        assertEquals(5, workerAddress(5, 2).getAgentIndex());
    }

    @Test
    public void testGetWorkerIndex() {
        assertEquals(5, workerAddress(5, 6).getAgentIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAddressIndex_fromCoordinator() {
        coordinatorAddress().getAddressIndex();
    }

    @Test
    public void testGetAddressIndex_fromAgent() {
        assertEquals(5, agentAddress(5).getAddressIndex());
    }

    @Test
    public void testGetAddressIndex_fromWorker() {
        assertEquals(6, workerAddress(1, 6).getAddressIndex());
    }


    @Test(expected = IllegalArgumentException.class)
    public void getParent_fromCoordinator() {
        coordinatorAddress().getParent();
    }

    @Test
    public void getParent_fromAgent() {
        assertEquals(coordinatorAddress(), agentAddress(1).getParent());
    }

    @Test
    public void getParent_fromWorker() {
        assertEquals(agentAddress(1), workerAddress(1, 5).getParent());
    }

    @Test
    public void testEquals() {
        assertEquals(workerAddress(1, 1), workerAddress(1, 1));
        assertNotEquals(workerAddress(1, 1), workerAddress(1, 2));
        assertNotEquals(workerAddress(1, 1), workerAddress(2, 1));
        assertNotEquals(workerAddress(1, 1), agentAddress(1));
        assertNotEquals(workerAddress(1, 1), coordinatorAddress());

        assertEquals(agentAddress(1), agentAddress(1));
        assertNotEquals(agentAddress(1), agentAddress(2));
        assertNotEquals(agentAddress(1), workerAddress(1, 2));
        assertNotEquals(agentAddress(1), coordinatorAddress());

        assertEquals(coordinatorAddress(), coordinatorAddress());
        assertNotEquals(coordinatorAddress(), agentAddress(1));
        assertNotEquals(coordinatorAddress(), workerAddress(1, 2));
    }

    @Test
    public void testHashcode() {
        assertEqualsHash(workerAddress(1, 1), workerAddress(1, 1));
        assertNotEqualsHash(workerAddress(1, 1), workerAddress(1, 2));
        assertNotEqualsHash(workerAddress(1, 1), workerAddress(2, 1));
        assertNotEqualsHash(workerAddress(1, 1), agentAddress(1));
        assertNotEqualsHash(workerAddress(1, 1), coordinatorAddress());

        assertEqualsHash(agentAddress(1), agentAddress(1));
        assertNotEqualsHash(agentAddress(1), agentAddress(2));
        assertNotEqualsHash(agentAddress(1), workerAddress(1, 2));
        assertNotEqualsHash(agentAddress(1), coordinatorAddress());

        assertEqualsHash(coordinatorAddress(), coordinatorAddress());
        assertNotEqualsHash(coordinatorAddress(), agentAddress(1));
        assertNotEqualsHash(coordinatorAddress(), workerAddress(1, 2));
    }

    private static void assertEqualsHash(SimulatorAddress a, SimulatorAddress b) {
        assertEquals(a.hashCode(), b.hashCode());
    }

    private static void assertNotEqualsHash(SimulatorAddress a, SimulatorAddress b) {
        assertNotEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void testToString_whenCoordinator() {
        assertEquals("C", coordinatorAddress().toString());
    }

    @Test
    public void testToString_whenAgent() {
        assertEquals("A5", agentAddress(5).toString());
    }

    @Test
    public void testToString_whenWorker() {
        assertEquals("A5_W2", workerAddress(5, 2).toString());
    }

    @Test
    public void testFromString_Coordinator() {
        assertToAndFromStringEquals(coordinatorAddress());
    }

    @Test
    public void testFromString_Agent() {
        assertToAndFromStringEquals(agentAddress(5));
    }

    @Test
    public void testFromString_Worker() {
        assertToAndFromStringEquals(workerAddress(3, 7));
    }

    private void assertToAndFromStringEquals(SimulatorAddress expectedAddress) {
        String addressString = expectedAddress.toString();
        SimulatorAddress actualAddress = SimulatorAddress.fromString(addressString);
        assertEquals(expectedAddress, actualAddress);
    }
}
