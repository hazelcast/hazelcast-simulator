package com.hazelcast.simulator.protocol.core;

/**
 * Defines the address level of a Simulator component.
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
public enum AddressLevel {

    COORDINATOR(0),
    AGENT(1),
    WORKER(2),
    TEST(3);

    private final int ordinal;

    AddressLevel(int ordinal) {
        this.ordinal = ordinal;
    }

    public static AddressLevel fromInt(int intValue) {
        switch (intValue) {
            case 0:
                return COORDINATOR;
            case 1:
                return AGENT;
            case 2:
                return WORKER;
            case 3:
                return TEST;
            default:
                throw new IllegalArgumentException("Unknown address level: " + intValue);
        }
    }

    public int toInt() {
        return ordinal;
    }

    public boolean isParentAddressLevel(AddressLevel addressLevel) {
        return (this.ordinal > addressLevel.ordinal);
    }
}
