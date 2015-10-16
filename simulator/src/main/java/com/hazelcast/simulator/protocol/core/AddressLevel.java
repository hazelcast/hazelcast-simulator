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
@SuppressWarnings("checkstyle:magicnumber")
public enum AddressLevel {

    COORDINATOR(0),
    AGENT(1),
    WORKER(2),
    TEST(3);

    private static final AddressLevel[] ADDRESS_LEVELS = new AddressLevel[]{COORDINATOR, AGENT, WORKER, TEST};

    private final int intValue;

    AddressLevel(int intValue) {
        this.intValue = intValue;
    }

    public static AddressLevel fromInt(int intValue) {
        if (intValue < 0 || intValue > ADDRESS_LEVELS.length - 1) {
            throw new IllegalArgumentException("Unknown address level: " + intValue);
        }
        return ADDRESS_LEVELS[intValue];
    }

    public int toInt() {
        return intValue;
    }

    public boolean isParentAddressLevel(AddressLevel addressLevel) {
        return (this.intValue < addressLevel.intValue);
    }
}
