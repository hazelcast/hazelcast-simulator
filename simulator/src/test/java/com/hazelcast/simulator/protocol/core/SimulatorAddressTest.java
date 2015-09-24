package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

public class SimulatorAddressTest {

    private final SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 5, 6, 7);
    private final SimulatorAddress addressSame = new SimulatorAddress(AddressLevel.TEST, 5, 6, 7);

    private final SimulatorAddress addressOtherAgent = new SimulatorAddress(AddressLevel.TEST, 9, 6, 7);
    private final SimulatorAddress addressOtherWorker = new SimulatorAddress(AddressLevel.TEST, 5, 9, 7);
    private final SimulatorAddress addressOtherTest = new SimulatorAddress(AddressLevel.TEST, 5, 6, 9);

    private final SimulatorAddress addressAgentAddressLevel = new SimulatorAddress(AddressLevel.AGENT, 5, 6, 7);
    private final SimulatorAddress addressWorkerAddressLevel = new SimulatorAddress(AddressLevel.WORKER, 5, 6, 7);

    @Test
    public void testGetAddressLevel() {
        assertEquals(AddressLevel.TEST, address.getAddressLevel());
    }

    @Test
    public void testGetAgentIndex() {
        assertEquals(5, address.getAgentIndex());
    }

    @Test
    public void testGetWorkerIndex() {
        assertEquals(6, address.getWorkerIndex());
    }

    @Test
    public void testGetTestIndex() {
        assertEquals(7, address.getTestIndex());
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
        assertEquals(7, address.getAddressIndex());
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
        assertEquals(AddressLevel.WORKER, address.getParent().getAddressLevel());
    }

    @Test
    public void getChild_fromCoordinator() {
        assertEquals(new SimulatorAddress(AddressLevel.AGENT, 3, 0, 0), SimulatorAddress.COORDINATOR.getChild(3));
    }

    @Test
    public void getChild_fromAgent() {
        assertEquals(new SimulatorAddress(AddressLevel.WORKER, 5, 9, 0), addressAgentAddressLevel.getChild(9));
    }

    @Test
    public void getChild_fromWorker() {
        assertEquals(new SimulatorAddress(AddressLevel.TEST, 5, 6, 2), addressWorkerAddressLevel.getChild(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void getChild_fromTest() {
        address.getChild(1);
    }

    @Test
    public void testEquals() {
        assertEquals(address, address);

        assertNotEquals(address, null);
        assertNotEquals(address, new Object());

        assertNotEquals(address, addressOtherAgent);
        assertNotEquals(address, addressOtherWorker);
        assertNotEquals(address, addressOtherTest);
        assertNotEquals(address, addressWorkerAddressLevel);

        assertEquals(address, addressSame);
    }

    @Test
    public void testHashcode() {
        assertNotEquals(address.hashCode(), addressOtherAgent.hashCode());
        assertNotEquals(address.hashCode(), addressOtherWorker.hashCode());
        assertNotEquals(address.hashCode(), addressOtherTest.hashCode());
        assertNotEquals(address.hashCode(), addressWorkerAddressLevel.hashCode());

        assertEquals(address.hashCode(), addressSame.hashCode());
    }

    @Test
    public void testToString() {
        assertNotNull(address.toString());
    }
}
