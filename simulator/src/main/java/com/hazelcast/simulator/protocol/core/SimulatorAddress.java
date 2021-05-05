/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.simulator.protocol.core.AddressLevel.AGENT;
import static com.hazelcast.simulator.protocol.core.AddressLevel.COORDINATOR;
import static com.hazelcast.simulator.protocol.core.AddressLevel.WORKER;
import static com.hazelcast.simulator.utils.Preconditions.checkPositive;
import static java.lang.Integer.parseInt;
import static java.lang.String.format;

/**
 * Address object which (uniquely) identifies one or more Simulator components.
 *
 * <pre>
 *                                               +---+
 * COORDINATOR           +-----------------------+ C +----------------------+
 *                       |                       +---+                      |
 *                       |                                                  |
 *  *                       v                                                  v
 *  *                    +--+---+                                           +---+--+
 *  * AGENT              |  A1  |                              +------------+  A2  +------------+
 *  *                    +--+---+                              |            +------+            |
 *  *                       |                                  |                                |
 *  *                       v                                  v                                v
 *  *                  +----+----+                        +----+----+                      +----+----+
 *  * WORKER           +  A1_W1  +                        +  A2_W1  +                      +  A2_W2  +
 *  *                  +---------+                        +---------+                      +---------+
 * </pre>
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class SimulatorAddress {

    private static final SimulatorAddress COORDINATOR_ADDRESS = new SimulatorAddress(COORDINATOR, 0, 0);
    private static final String COORDINATOR_STRING = "C";

    private final AddressLevel addressLevel;
    private final int agentIndex;
    private final int workerIndex;

    private SimulatorAddress(AddressLevel addressLevel, int agentIndex, int workerIndex) {
        this.addressLevel = addressLevel;
        this.agentIndex = agentIndex;
        this.workerIndex = workerIndex;
    }

    public static SimulatorAddress coordinatorAddress() {
        return COORDINATOR_ADDRESS;
    }

    public static SimulatorAddress agentAddress(int agentIndex) {
        return new SimulatorAddress(AGENT, checkPositive(agentIndex, "agentIndex"), 0);
    }

    public static SimulatorAddress workerAddress(int agentIndex, int workerIndex) {
        return new SimulatorAddress(WORKER, checkPositive(agentIndex, "agentIndex"), checkPositive(workerIndex, "workerIndex"));
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
            case WORKER:
                return agentAddress(agentIndex);
            case AGENT:
                return coordinatorAddress();
            default:
                throw new IllegalArgumentException("Coordinator has no parent!");
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
        return (addressLevel == that.addressLevel);
    }

    @Override
    public int hashCode() {
        int result = addressLevel.hashCode();
        result = 31 * result + agentIndex;
        result = 31 * result + workerIndex;
        return result;
    }

    @Override
    public String toString() {
        if (addressLevel == COORDINATOR) {
            return COORDINATOR_STRING;
        } else if (addressLevel == AGENT) {
            return "A" + agentIndex;
        } else {
            return "A" + agentIndex + "_W" + workerIndex;
        }
    }

    public static SimulatorAddress fromString(String sourceString) {
        if (COORDINATOR_STRING.equals(sourceString)) {
            return coordinatorAddress();
        }

        if (!sourceString.startsWith("A")) {
            throw new IllegalArgumentException(format("'%s' is not a valid address", sourceString));
        }

        int indexOfSplit = sourceString.indexOf("_W");
        if (indexOfSplit == -1) {
            int agentIndex = parseInt(sourceString.substring(1, sourceString.length()));
            return agentAddress(agentIndex);
        } else {
            int agentIndex = parseInt(sourceString.substring(1, indexOfSplit));
            int workerIndex = parseInt(sourceString.substring(indexOfSplit + 2, sourceString.length()));
            return workerAddress(agentIndex, workerIndex);
        }
    }

    public static List<SimulatorAddress> fromString(List<String> list) {
        List<SimulatorAddress> result = new ArrayList<>(list.size());
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
}
