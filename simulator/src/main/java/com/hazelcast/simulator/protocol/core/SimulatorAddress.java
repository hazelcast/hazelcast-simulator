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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static java.lang.Integer.parseInt;

/**
 * Address object which (uniquely) identifies one or more Simulator components.
 *
 * Supports wildcards on each {@link AddressLevel} to target all components on that address level.
 * For example a {@link SimulatorMessage} to <tt>C_A2_W*_T1</tt> will be sent to <tt>C_A2_W1_T1</tt> and <tt>C_A2_W2_T1</tt>.
 *
 * <pre>
 *                                               +---+
 * REMOTE                                        + R +
 *                                               +---+
 *                                                 |
 *                                                 v
 *                                               +---+
 * COORDINATOR           +-----------------------+ C +----------------------+
 *                       |                       +---+                      |
 *                       |                                                  |
 *                       v                                                  v
 *                    +--+---+                                           +---+--+
 * AGENT              | C_A1 |                              +------------+ C_A2 +------------+
 *                    +--+---+                              |            +------+            |
 *                       |                                 |                                 |
 *                       v                                 v                                 v
 *                  +----+----+                       +----+----+                       +----+----+
 * WORKER       +---+ C_A1_W1 +---+               +---+ C_A2_W1 +---+               +---+ C_A2_W2 +---+
 *              |   +---------+   |               |   +---------+   |               |   +---------+   |
 *              |                 |               |                 |               |                 |
 *              v                 v               v                 v               v                 v
 *        +-----+------+   +------+-----+   +-----+------+   +------+-----+   +-----+------+   +------+-----+
 * TEST   | C_A1_W1_T1 |   | C_A1_W1_T2 |   | C_A2_W1_T1 |   | C_A2_W1_T2 |   | C_A2_W2_T1 |   | C_A2_W2_T2 |
 *        +------------+   +------------+   +------------+   +------------+   +------------+   +------------+
 * </pre>
 */
@SuppressWarnings("checkstyle:magicnumber")
public class SimulatorAddress {

    public static final SimulatorAddress REMOTE = new SimulatorAddress(AddressLevel.REMOTE, 0, 0, 0);
    public static final SimulatorAddress COORDINATOR = new SimulatorAddress(AddressLevel.COORDINATOR, 0, 0, 0);
    public static final SimulatorAddress ALL_AGENTS = new SimulatorAddress(AddressLevel.AGENT, 0, 0, 0);
    public static final SimulatorAddress ALL_WORKERS = new SimulatorAddress(AddressLevel.WORKER, 0, 0, 0);

    private static final String REMOTE_STRING = "R";
    private static final String COORDINATOR_STRING = "C";

    private final AddressLevel addressLevel;
    private final int agentIndex;
    private final int workerIndex;
    private final int testIndex;

    /**
     * Creates a new {@link SimulatorAddress} instance.
     *
     * @param addressLevel the {@link AddressLevel} of the Simulator component
     * @param agentIndex   the index of the addressed Agent or <tt>0</tt> for all Agents
     * @param workerIndex  the index of the addressed Worker or <tt>0</tt> for all Workers
     * @param testIndex    the index of the addressed Test or <tt>0</tt> for all Tests
     */
    public SimulatorAddress(AddressLevel addressLevel, int agentIndex, int workerIndex, int testIndex) {
        this.addressLevel = addressLevel;
        this.agentIndex = agentIndex;
        this.workerIndex = workerIndex;
        this.testIndex = testIndex;
    }

    /**
     * Returns the {@link AddressLevel} of this {@link SimulatorAddress}.
     *
     * @return the {@link AddressLevel}
     */
    public AddressLevel getAddressLevel() {
        return addressLevel;
    }

    /**
     * Returns the Agent index of this {@link SimulatorAddress}.
     *
     * @return the index of the addressed Agent or <tt>0</tt> if all Agents are addressed
     */
    public int getAgentIndex() {
        return agentIndex;
    }

    /**
     * Returns the Worker index of this {@link SimulatorAddress}.
     *
     * @return the index of the addressed Worker or <tt>0</tt> if all Workers are addressed
     */
    public int getWorkerIndex() {
        return workerIndex;
    }

    /**
     * Returns the Test index of this {@link SimulatorAddress}.
     *
     * @return the index of the addressed Test or <tt>0</tt> if all Tests are addressed
     */
    public int getTestIndex() {
        return testIndex;
    }

    /**
     * Returns the address index of the defined {@link AddressLevel} of this {@link SimulatorAddress}.
     *
     * @return the index of the {@link AddressLevel}
     */
    public int getAddressIndex() {
        switch (addressLevel) {
            case AGENT:
                return agentIndex;
            case WORKER:
                return workerIndex;
            case TEST:
                return testIndex;
            default:
                throw new IllegalArgumentException("Coordinator has no addressIndex!");
        }
    }

    /**
     * Returns the {@link SimulatorAddress} of the parent Simulator component.
     *
     * @return the {@link SimulatorAddress} of the parent Simulator component
     */
    public SimulatorAddress getParent() {
        switch (addressLevel) {
            case TEST:
                return new SimulatorAddress(AddressLevel.WORKER, agentIndex, workerIndex, 0);
            case WORKER:
                return new SimulatorAddress(AddressLevel.AGENT, agentIndex, 0, 0);
            case AGENT:
                return SimulatorAddress.COORDINATOR;
            default:
                throw new IllegalArgumentException("Coordinator has no parent!");
        }
    }

    /**
     * Returns the {@link SimulatorAddress} of the addressed child of this Simulator component.
     *
     * @param childIndex the addressIndex of the child
     * @return the {@link SimulatorAddress} of the addressed child of this Simulator component
     */
    public SimulatorAddress getChild(int childIndex) {
        switch (addressLevel) {
            case COORDINATOR:
                return new SimulatorAddress(AddressLevel.AGENT, childIndex, 0, 0);
            case AGENT:
                return new SimulatorAddress(AddressLevel.WORKER, agentIndex, childIndex, 0);
            case WORKER:
                return new SimulatorAddress(AddressLevel.TEST, agentIndex, workerIndex, childIndex);
            default:
                throw new IllegalArgumentException("Test has no child!");
        }
    }

    /**
     * Checks if the {@link SimulatorAddress} contains a wildcard.
     *
     * @return {@code true} if the {@link SimulatorAddress} contains a wildcard, {@code false} otherwise.
     */
    public boolean containsWildcard() {
        switch (addressLevel) {
            case AGENT:
                return agentIndex == 0;
            case WORKER:
                return agentIndex == 0 || workerIndex == 0;
            case TEST:
                return agentIndex == 0 || workerIndex == 0 || testIndex == 0;
            default:
                return false;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SimulatorAddress that = (SimulatorAddress) o;
        if (agentIndex != that.agentIndex) {
            return false;
        }
        if (workerIndex != that.workerIndex) {
            return false;
        }
        if (testIndex != that.testIndex) {
            return false;
        }
        return (addressLevel == that.addressLevel);
    }

    @Override
    public int hashCode() {
        int result = addressLevel.hashCode();
        result = 31 * result + agentIndex;
        result = 31 * result + workerIndex;
        result = 31 * result + testIndex;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (AddressLevel.REMOTE == addressLevel) {
            sb.append(REMOTE_STRING);
        } else {
            sb.append(COORDINATOR_STRING);
        }
        appendAddressLevelString(sb, AddressLevel.COORDINATOR, "_A", agentIndex);
        appendAddressLevelString(sb, AddressLevel.AGENT, "_W", workerIndex);
        appendAddressLevelString(sb, AddressLevel.WORKER, "_T", testIndex);
        return sb.toString();
    }

    private void appendAddressLevelString(StringBuilder sb, AddressLevel parent, String name, int index) {
        if (parent.isParentAddressLevel(addressLevel)) {
            sb.append(name).append(index == 0 ? "*" : index);
        }
    }

    public static SimulatorAddress fromString(String sourceString) {
        String[] sections = sourceString.split("_");
        AddressLevel addressLevel = AddressLevel.fromInt(sections.length - 1);
        if (addressLevel == AddressLevel.COORDINATOR) {
            return REMOTE_STRING.equals(sourceString) ? REMOTE : COORDINATOR;
        }

        int agentIndex = getAddressIndex(AddressLevel.COORDINATOR, addressLevel, "A*", sections);
        int workerIndex = getAddressIndex(AddressLevel.AGENT, addressLevel, "W*", sections);
        int testIndex = getAddressIndex(AddressLevel.WORKER, addressLevel, "T*", sections);

        return new SimulatorAddress(addressLevel, agentIndex, workerIndex, testIndex);
    }

    public static List<SimulatorAddress> fromString(List<String> list) {
        List<SimulatorAddress> result = new ArrayList<SimulatorAddress>(list.size());
        for (String address : list) {
            result.add(fromString(address));
        }
        return result;
    }

    public static String toString(Collection<SimulatorAddress> addresses) {
        StringBuilder sb = new StringBuilder();

        boolean first = true;
        for (SimulatorAddress address : addresses) {
            if (first) {
                first = false;
            } else {
                sb.append(",");
            }
            sb.append(address);
        }

        return sb.toString();
    }

    private static int getAddressIndex(AddressLevel parentLevel, AddressLevel level, String wildcard, String[] sections) {
        if (!parentLevel.isParentAddressLevel(level)) {
            return 0;
        }
        int sectionsIndex = parentLevel.toInt() + 1;
        return wildcard.equals(sections[sectionsIndex]) ? 0 : parseInt(sections[sectionsIndex].substring(1));
    }
}
