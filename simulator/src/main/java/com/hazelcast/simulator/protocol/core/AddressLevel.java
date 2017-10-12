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

/**
 * Defines the address level of a Simulator component.
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
public enum AddressLevel {

    REMOTE(-1),
    COORDINATOR(0),
    AGENT(1),
    WORKER(2),
    TEST(3);

    private static final AddressLevel[] ADDRESS_LEVELS = new AddressLevel[]{REMOTE, COORDINATOR, AGENT, WORKER, TEST};

    private final int intValue;

    AddressLevel(int intValue) {
        this.intValue = intValue;
    }

    public static int getMinLevel() {
        return REMOTE.intValue;
    }

    public static int getMaxLevel() {
        return TEST.intValue;
    }

    public static AddressLevel fromInt(int intValue) {
        if (intValue < getMinLevel() || intValue > getMaxLevel()) {
            throw new IllegalArgumentException("Unknown address level: " + intValue);
        }
        return ADDRESS_LEVELS[intValue + 1];
    }

    public int toInt() {
        return intValue;
    }

    public boolean isParentAddressLevel(AddressLevel addressLevel) {
        return (this.intValue < addressLevel.intValue);
    }
}
