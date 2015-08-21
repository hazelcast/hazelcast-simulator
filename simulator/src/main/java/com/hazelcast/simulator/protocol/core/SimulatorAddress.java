package com.hazelcast.simulator.protocol.core;

/**
 * Address object which (uniquely) identifies one or more Simulator components.
 *
 * Supports wildcards on each {@link AddressLevel} to target all components on that address level.
 * For example a {@link SimulatorMessage} to <tt>C_A2_W*_T1</tt> will be sent to <tt>C_A2_W1_T1</tt> and <tt>C_A2_W2_T1</tt>.
 *
 * <pre>
 *                                               +---+
 * COORDINATOR           +-----------------------+ C +----------------------+
 *                       |                       +---+                      |
 *                       |                                                  |
 *                       v                                                  v
 *                     +-+--+                                            +--+-+
 * AGENT               | A1 |                              +-------------+ A2 +--------------+
 *                     +-+--+                              |             +----+              |
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
public class SimulatorAddress {

    public static final SimulatorAddress COORDINATOR = new SimulatorAddress(AddressLevel.COORDINATOR, 0, 0, 0);

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
        StringBuilder sb = new StringBuilder("C");
        if (AddressLevel.COORDINATOR.isParentAddressLevel(addressLevel)) {
            sb.append("_A").append(agentIndex == 0 ? "*" : agentIndex);
        }
        if (AddressLevel.AGENT.isParentAddressLevel(addressLevel)) {
            sb.append("_W").append(workerIndex == 0 ? "*" : workerIndex);
        }
        if (AddressLevel.WORKER.isParentAddressLevel(addressLevel)) {
            sb.append("_T").append(testIndex == 0 ? "*" : testIndex);
        }
        return sb.toString();
    }
}
