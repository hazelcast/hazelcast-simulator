/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package com.hazelcast.simulator.tests.map.helpers;

import org.apache.log4j.Logger;

import static com.hazelcast.simulator.tests.map.helpers.ZipfianUtils.random;
import static java.lang.String.format;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than others,
 * according to a zipfian distribution. When you construct an instance of this class, you specify the number of items in the set
 * to draw from, either by specifying an itemCount (so that the sequence is of items from 0 to itemCount - 1) or by specifying a
 * min and a max (so that the sequence is of items from min to max inclusive). After you construct the instance, you can change
 * the number of items by calling {@link #nextInt()} or {@link #nextLong()}.
 *
 * Note that the popular items will be clustered together, e.g. item 0 is the most popular, item 1 the second most popular, and so
 * on (or min is the most popular, min + 1 the next most popular, etc.) If you don't want this clustering, and instead want the
 * popular items scattered throughout the item space, then use {@link ScrambledZipfianGenerator} instead.
 *
 * Be aware: initializing this generator may take a long time if there are lots of items to choose from (e.g. over a minute for
 * 100 million objects). This is because certain mathematical values need to be computed to properly generate a zipfian skew, and
 * one of those values (zeta) is a sum sequence from 1 to n, where n is the itemCount. Note that if you increase the number of
 * items in the set, we can compute a new zeta incrementally, so it should be fast unless you have added millions of items.
 * However, if you decrease the number of items, we recompute zeta from scratch, so this can take a long time.
 *
 * The algorithm used here is from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994.
 */
@SuppressWarnings("unused")
public class ZipfianGenerator extends IntegerGenerator {

    static final double ZIPFIAN_CONSTANT = 0.99;

    private static final Logger LOGGER = Logger.getLogger(ZipfianGenerator.class);

    /**
     * Number of items.
     */
    long items;

    /**
     * Min item to generate.
     */
    long base;

    /**
     * Computed parameters for generating the distribution.
     */
    double alpha;
    double zeta;
    double eta;
    double theta;
    double zeta2theta;

    /**
     * The number of items used to compute zeta the last time.
     */
    volatile long countForZeta;

    /**
     * Flag to prevent problems. If you increase the number of items the zipfian generator is allowed to choose from, this code
     * will incrementally compute a new zeta value for the larger itemCount. However, if you decrease the number of items, the
     * code computes zeta from scratch; this is expensive for large item sets. Usually this is not intentional; e.g. one thread
     * thinks the number of items is 1001 and calls {@link #nextLong()} with that item count; then another thread who thinks the
     * number of items is 1000 calls {@link #nextLong()} with itemCount = 1000 triggering the expensive recomputation. (It is
     * expensive for 100 million items, not really for 1000 items.) Why did the second thread think there were only 1000 items?
     * Maybe it read the item count before the first thread incremented it. So this flag allows you to say if you really do want
     * that recomputation. If true, then the code will recompute zeta if the itemCount goes down. If false, the code will assume
     * itemCount only goes up, and never recompute.
     */
    boolean allowItemCountDecrease;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     *
     * @param items The number of items in the distribution.
     */
    public ZipfianGenerator(long items) {
        this(0, items - 1);
    }

    /**
     * Create a zipfian generator for items between min and max.
     *
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     */
    public ZipfianGenerator(long min, long max) {
        this(min, max, ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param items           The number of items in the distribution.
     * @param zipfianConstant The zipfian constant to use.
     */
    public ZipfianGenerator(long items, double zipfianConstant) {
        this(0, items - 1, zipfianConstant);
    }

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant.
     *
     * @param min             The smallest integer to generate in the sequence.
     * @param max             The largest integer to generate in the sequence.
     * @param zipfianConstant The zipfian constant to use.
     */
    public ZipfianGenerator(long min, long max, double zipfianConstant) {
        this(min, max, zipfianConstant, zetaStatic(max - min + 1, zipfianConstant));
    }

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant, using the
     * precomputed value of zeta.
     *
     * @param min             The smallest integer to generate in the sequence.
     * @param max             The largest integer to generate in the sequence.
     * @param zipfianConstant The zipfian constant to use.
     * @param zeta            The precomputed zeta constant.
     */
    public ZipfianGenerator(long min, long max, double zipfianConstant, double zeta) {
        items = max - min + 1;
        base = min;

        theta = zipfianConstant;

        zeta2theta = zeta(2, theta);

        alpha = 1.0 / (1.0 - theta);
        this.zeta = zeta;
        countForZeta = items;
        eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / this.zeta);
        nextInt();
    }

    /**************************************************************************/

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items, using the
     * zipfian constant theta. Remember the value of n, so if we change the itemCount, we can recompute zeta.
     *
     * @param n     The number of items to compute zeta over.
     * @param theta The zipfian constant.
     */
    double zeta(long n, double theta) {
        countForZeta = n;
        return zetaStatic(n, theta);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items, using the
     * zipfian constant theta. This is a static version of the function which will not remember n.
     *
     * @param n     The number of items to compute zeta over.
     * @param theta The zipfian constant.
     */
    static double zetaStatic(long n, double theta) {
        return zetaStatic(0, n, theta, 0);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that has n items now but
     * used to have st items. Use the zipfian constant theta. Remember the new value of n so that if we change the itemCount,
     * we'll know to recompute zeta.
     *
     * @param st         The number of items used to compute the last initialSum
     * @param n          The number of items to compute zeta over.
     * @param theta      The zipfian constant.
     * @param initialSum The value of zeta we are computing incrementally from.
     */
    double zeta(long st, long n, double theta, double initialSum) {
        countForZeta = n;
        return zetaStatic(st, n, theta, initialSum);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that has n items now but
     * used to have st items. Use the zipfian constant theta. Remember the new value of n so that if we change the itemCount,
     * we'll know to recompute zeta.
     *
     * @param st         The number of items used to compute the last initialSum
     * @param n          The number of items to compute zeta over.
     * @param theta      The zipfian constant.
     * @param initialSum The value of zeta we are computing incrementally from.
     */
    static double zetaStatic(long st, long n, double theta, double initialSum) {
        double sum = initialSum;
        for (long i = st; i < n; i++) {

            sum += 1 / (Math.pow(i + 1, theta));
        }
        return sum;
    }

    /****************************************************************************************/

    /**
     * Generate the next item. this distribution will be skewed toward lower integers; e.g. 0 will be the most popular, 1 the
     * next most popular, etc.
     *
     * @param itemCount The number of items in the distribution.
     * @return The next item in the sequence.
     */
    public int nextInt(int itemCount) {
        return (int) nextLong(itemCount);
    }

    /**
     * Generate the next item as a long.
     *
     * @param itemCount The number of items in the distribution.
     * @return The next item in the sequence.
     */
    public long nextLong(long itemCount) {
        // from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994

        if (itemCount != countForZeta) {
            synchronized (this) {
                if (itemCount != countForZeta) {
                    // have to recompute zeta and eta, since they depend on itemCount
                    recomputeZetaAndEta(itemCount);
                    return computeNextLong(itemCount);
                }
            }
        }

        return computeNextLong(itemCount);
    }

    private void recomputeZetaAndEta(long itemCount) {
        if (itemCount > countForZeta) {

            // we have added more items. can compute zeta incrementally, which is cheaper
            zeta = zeta(countForZeta, itemCount, theta, zeta);
            eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zeta);
        } else if ((itemCount < countForZeta) && (allowItemCountDecrease)) {
            // have to start over with zeta
            // note: for large item sets, this is very slow. so don't do it!

            // TODO: can also have a negative incremental computation, e.g. if you decrease the number of items, then just
            // subtract the zeta sequence terms for the items that went away. This would be faster than recomputing from
            // scratch when the number of items decreases

            LOGGER.warn(format("Recomputing Zipfian distribution. This is slow and should be avoided."
                    + " (itemCount=%d countForZeta=%d)", itemCount, countForZeta));

            zeta = zeta(itemCount, theta);
            eta = (1 - Math.pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zeta);
        }
    }

    private long computeNextLong(long itemCount) {
        double u = random().nextDouble();
        double uz = u * zeta;

        if (uz < 1.0) {
            return 0;
        }

        if (uz < 1.0 + Math.pow(0.5, theta)) {
            return 1;
        }

        long ret = base + (long) ((itemCount) * Math.pow(eta * u - eta + 1, alpha));
        setLastInt((int) ret);
        return ret;

    }

    /**
     * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by the 1st,
     * followed by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the next most popular,
     * etc.) If you want the popular items scattered throughout the item space, use ScrambledZipfianGenerator instead.
     */
    @Override
    public final int nextInt() {
        return (int) nextLong(items);
    }

    /**
     * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by the 1st,
     * followed by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the next most popular,
     * etc.) If you want the popular items scattered throughout the item space, use ScrambledZipfianGenerator instead.
     *
     * @return the next value of the Zipfian distribution
     */
    public long nextLong() {
        return nextLong(items);
    }

    @Override
    public double mean() {
        throw new UnsupportedOperationException("Not implemented!");
    }
}
