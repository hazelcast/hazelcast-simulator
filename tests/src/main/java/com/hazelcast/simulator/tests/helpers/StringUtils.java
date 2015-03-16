package com.hazelcast.simulator.tests.helpers;

import java.util.Random;

public class StringUtils {
    private static final String alphabet = "abcdefghijklmnopqrstuvwxyz1234567890";
    private static final Random random = new Random();

    /**
     * Generates an array of strings.
     *
     * @param count number of String in the array
     * @param length the length of each individual string
     * @return the created array of Strings.
     */
    public static String[] generateStrings(int count, int length) {
        String[] keys = new String[count];
        for (int k = 0; k < keys.length; k++) {
            keys[k] = generateString(length);
        }
        return keys;
    }

    public static String generateString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int k = 0; k < length; k++) {
            char c = alphabet.charAt(random.nextInt(alphabet.length()));
            sb.append(c);
        }

        return sb.toString();
    }
}
