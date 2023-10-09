package com.hazelcast.simulator.utils;

import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class HotSetKeySelectorTest {
    private static final int SELECTIONS = 10_000_000;

    @Parameters(name = "max:{0}, hsAccPerc: {1}, hsPerc: {2}")
    public static Collection primeNumbers() {
        return Arrays.asList(new Object[][]{
                {100, 80, 20, 4.0D, 0.25D, 10E-0, 10E-2},
                {100, 95, 10, 9.50D, 0.055D, 10E-1, 10E-3},
                {1000, 99, 30, 0.33D, 0.0014D, 10E-2, 10E-4},
        });
    }

    /**
     * The size of the key domain to be tested.
     */
    @Parameter(0)
    public int keyDomain;

    /**
     * The percentage of all accesses hitting the hot set.
     */
    @Parameter(1)
    public int hotSetAccessPercentage;

    /**
     * The percentage of all accesses hitting the cold set.
     */
    @Parameter(2)
    public int hotSetPercentage;

    /**
     * The expected percentage of hitting a single key in the hot set.
     */
    @Parameter(3)
    public double expectedHotKeyHitPercentage;

    /**
     * The expected percentage of hitting a single key in the cold set.
     */
    @Parameter(4)
    public double expectedColdKeyHitPercentage;

    /**
     * The epsilon defining the accepted interval around {@link #expectedHotKeyHitPercentage}.
     */
    @Parameter(5)
    public double hotKeyPercentageEpsilon;

    /**
     * The epsilon defining the accepted interval around {@link #expectedColdKeyHitPercentage}.
     */
    @Parameter(6)
    public double coldKeyPercentageEpsilon;

    @Test
    public void test() {
        HotSetKeySelector keySelector = new HotSetKeySelector(0, keyDomain - 1, hotSetAccessPercentage, hotSetPercentage);
        long[] dist = runSelections(keySelector);

        verifyDistribution(dist, keySelector.getHotSetThreshold());
    }

    @NotNull
    private long[] runSelections(HotSetKeySelector keySelector) {
        long[] dist = new long[keyDomain];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < SELECTIONS; i++) {
            long x = keySelector.nextKey(random::nextLong);
            dist[(int) x]++;
        }
        return dist;
    }

    private void verifyDistribution(long[] dist, int hotSetThreshold) {
        long coldSetHits = 0;
        long hotSetHits = 0;
        for (int i = 0; i < dist.length; i++) {
            long keyHits = dist[i];
            double keyHitPercentage = ((double) keyHits) / SELECTIONS * 100;
            if (i < hotSetThreshold) {
                assertEquals("Unexpected hit count for key: " + i, expectedColdKeyHitPercentage, keyHitPercentage,
                        coldKeyPercentageEpsilon);
                coldSetHits += keyHits;
            } else {
                assertEquals(expectedHotKeyHitPercentage, keyHitPercentage, hotKeyPercentageEpsilon);
                hotSetHits += keyHits;
            }
        }

        double coldSetHitsPercentage = ((double) coldSetHits) / SELECTIONS * 100;
        double hotSetHitsPercentage = ((double) hotSetHits) / SELECTIONS * 100;

        assertEquals(100.0D - hotSetAccessPercentage, coldSetHitsPercentage, 10E-1);
        assertEquals(hotSetAccessPercentage, hotSetHitsPercentage, 10E-1);
    }
}
