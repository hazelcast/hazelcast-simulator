package com.hazelcast.simulator.protocol.core;

/**
 * Defines constants for all other codec classes.
 */
@SuppressWarnings("checkstyle:magicnumber")
public final class BaseCodec {

    public static final int INT_SIZE = 4;
    public static final int LONG_SIZE = 8;

    public static final int ADDRESS_SIZE = 4 * INT_SIZE;

    private BaseCodec() {
    }
}
