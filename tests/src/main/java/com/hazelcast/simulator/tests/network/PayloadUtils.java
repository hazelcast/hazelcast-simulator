package com.hazelcast.simulator.tests.network;

public class PayloadUtils {

    private PayloadUtils() {
    }

    public static byte[] makePayload(int payloadSize) {
        if (payloadSize <= 0) {
            return null;
        }

        byte[] payload = new byte[payloadSize];

        // put a well known head and tail on the payload; for debugging.
        if (payload.length >= 6 + 8) {
            addHeadTailMarkers(payload);
        }

        return payload;
    }

    public static void addHeadTailMarkers(byte[] payload) {
        payload[0] = 0xA;
        payload[1] = 0xB;
        payload[2] = 0xC;

        int length = payload.length;
        payload[length - 3] = 0xC;
        payload[length - 2] = 0xB;
        payload[length - 1] = 0xA;
    }

    public static void checkHeadTailMarkers(byte[] payload) {
        check(payload, 0, 0XA);
        check(payload, 1, 0XB);
        check(payload, 2, 0XC);

        int length = payload.length;
        check(payload, length - 3, 0XC);
        check(payload, length - 2, 0XB);
        check(payload, length - 1, 0XA);
    }

    public static void check(byte[] payload, int index, int value) {
        if (payload[index] != value) {
            throw new IllegalStateException();
        }
    }
}
