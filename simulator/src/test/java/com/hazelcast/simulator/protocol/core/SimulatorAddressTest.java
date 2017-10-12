/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.protocol.core;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

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
    public void containsWildcard_withRemote() {
        assertFalse(SimulatorAddress.REMOTE.containsWildcard());
    }

    @Test
    public void containsWildcard_withCoordinator() {
        assertFalse(SimulatorAddress.COORDINATOR.containsWildcard());
    }

    @Test
    public void containsWildcard_withAgent() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.AGENT, 1, 0, 0);
        assertFalse(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withAgent_withWildcard() {
        assertTrue(SimulatorAddress.ALL_AGENTS.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 1, 1, 0);
        assertFalse(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withAgentWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 0, 1, 0);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withWorkerWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.WORKER, 1, 0, 0);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withWorker_withAllWildcards() {
        assertTrue(SimulatorAddress.ALL_WORKERS.containsWildcard());
    }

    @Test
    public void containsWildcard_withTest() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 1, 1, 1);
        assertFalse(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withTest_withAgentWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 0, 1, 1);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withTest_withWorkerWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 1, 0, 1);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withTest_withTestWildcard() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 1, 1, 0);
        assertTrue(address.containsWildcard());
    }

    @Test
    public void containsWildcard_withTest_withAllWildcards() {
        SimulatorAddress address = new SimulatorAddress(AddressLevel.TEST, 0, 0, 0);
        assertTrue(address.containsWildcard());
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

    @Test
    public void testFromString_RemoteController() {
        SimulatorAddress expectedAddress = SimulatorAddress.REMOTE;
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_Coordinator() {
        SimulatorAddress expectedAddress = SimulatorAddress.COORDINATOR;
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.AGENT, 5, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.AGENT, 0, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_singleWorker() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 3, 7, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_allWorkers() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 3, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_singleWorker() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 0, 8, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_allWorkers() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.WORKER, 0, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_singleWorker_singleTest() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 3, 2, 6);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_singleWorker_allTests() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 3, 2, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_allWorkers_singleTest() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 9, 0, 3);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_singleAgent_allWorkers_allTests() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 9, 0, 0);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_allWorkers_singleTest() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 0, 0, 17);
        assertToAndFromStringEquals(expectedAddress);
    }

    @Test
    public void testFromString_allAgents_allWorkers_allTests() {
        SimulatorAddress expectedAddress = new SimulatorAddress(AddressLevel.TEST, 0, 0, 108);
        assertToAndFromStringEquals(expectedAddress);
    }

    private void assertToAndFromStringEquals(SimulatorAddress expectedAddress) {
        String addressString = expectedAddress.toString();
        SimulatorAddress actualAddress = SimulatorAddress.fromString(addressString);
        assertEquals(expectedAddress, actualAddress);
    }
}
