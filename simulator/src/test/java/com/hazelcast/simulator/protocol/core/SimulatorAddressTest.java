package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SimulatorAddressTest {

    private final SimulatorAddress workerAddress = new SimulatorAddress(AddressLevel.WORKER, 5, 6);
    private final SimulatorAddress addressSame = new SimulatorAddress(AddressLevel.WORKER, 5, 6);

    private final SimulatorAddress addressOtherAgent = new SimulatorAddress(AddressLevel.WORKER, 9, 6);
    private final SimulatorAddress addressOtherWorker = new SimulatorAddress(AddressLevel.WORKER, 5, 9);

    private final SimulatorAddress addressAgentAddressLevel = new SimulatorAddress(AddressLevel.AGENT, 5, 6);
    private final SimulatorAddress addressWorkerAddressLevel = new SimulatorAddress(AddressLevel.WORKER, 5, 6);

    @Test
    public void testGetAddressLevel() {
        assertEquals(AddressLevel.WORKER, workerAddress.getAddressLevel());
    }

    @Test
    public void testGetAgentIndex() {
        assertEquals(5, workerAddress.getAgentIndex());
    }

    @Test
    public void testGetWorkerIndex() {
        assertEquals(6, workerAddress.getWorkerIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetAddressIndex_fromCoordinator() {
        SimulatorAddress.COORDINATOR.getAddressIndex();
    }

    @Test
    public void testGetAddressIndex_fromAgent() {
        assertEquals(5, addressAgentAddressLevel.getAddressIndex());
    }

    @Test
    public void testGetAddressIndex_fromWorker() {
        assertEquals(6, addressWorkerAddressLevel.getAddressIndex());
    }

    @Test
    public void testGetAddressIndex_fromTest() {
        assertEquals(6, workerAddress.getAddressIndex());
    }

    @Test(expected = IllegalArgumentException.class)
    public void getParent_fromCoordinator() {
        SimulatorAddress.COORDINATOR.getParent();
    }

    @Test
    public void getParent_fromAgent() {
        assertEquals(AddressLevel.COORDINATOR, addressAgentAddressLevel.getParent().getAddressLevel());
    }

    @Test
    public void getParent_fromWorker() {
        assertEquals(AddressLevel.AGENT, addressWorkerAddressLevel.getParent().getAddressLevel());
    }

    @Test
    public void getParent_fromTest() {
        assertEquals(AddressLevel.AGENT, workerAddress.getParent().getAddressLevel());
    }

    @Test
    public void getChild_fromCoordinator() {
        assertEquals(new SimulatorAddress(AddressLevel.AGENT, 3, 0), SimulatorAddress.COORDINATOR.getChild(3));
    }

    @Test
    public void getChild_fromAgent() {
        assertEquals(new SimulatorAddress(AddressLevel.WORKER, 5, 9), addressAgentAddressLevel.getChild(9));
    }


    @Test(expected = IllegalArgumentException.class)
    public void getChild_fromTest() {
        workerAddress.getChild(1);
    }

    @Test
    public void containsWildcard_withCoordinator() {
        assertFalse(SimulatorAddress.COORDINATOR.containsWildcard());
    }

    @Test
    public void containsWildcard_withAgent() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.AGENT, 1, 0);
        assertFalse(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withAgent_withWildcard() {
        assertTrue(SimulatorAddress.ALL_AGENTS.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 1, 1);
        assertFalse(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withAgentWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 0, 1);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withWorkerWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 1, 0);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withAllWildcards() {
        assertTrue(SimulatorAddress.ALL_WORKERS.containsWildcard());
    }


    @Test
    public void testEquals() {
        assertEquals(workerAddress, workerAddress);

        assertNotEquals(workerAddress, null);
        assertNotEquals(workerAddress, new Object());

        assertNotEquals(workerAddress, addressOtherAgent);
        assertNotEquals(workerAddress, addressOtherWorker);
        assertEquals(workerAddress, addressWorkerAddressLevel);

        assertEquals(workerAddress, addressSame);
    }

    @Test
    public void testHashcode() {
        assertNotEquals(workerAddress.hashCode(), addressOtherAgent.hashCode());
        assertNotEquals(workerAddress.hashCode(), addressOtherWorker.hashCode());
        assertEquals(workerAddress.hashCode(), addressWorkerAddressLevel.hashCode());

        assertEquals(workerAddress.hashCode(), addressSame.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(workerAddress.toString());
    }

    @Test
    public void testFromString_Coordinator() {
        SimulatorAddress expectedAddress = SimulatorAddress.COORDINATOR;
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.AGENT, 5, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.AGENT, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_singleWorker() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 3, 7);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_allWorkers() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 3, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_singleWorker() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 0, 8);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_allWorkers() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    private void assertToAndFromStringEquals(SimulatorAddress expectedAddress) {
        String addressString = expectedAddress.toString();
        SimulatorAddress actualAddress = SimulatorAddress.fromString(addressString);
        assertEquals(expectedAddress, actualAddress);
    }
}
