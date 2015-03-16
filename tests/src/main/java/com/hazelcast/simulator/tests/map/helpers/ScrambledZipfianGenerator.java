/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
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

import com.hazelcast.simulator.tests.helpers.IntegerGenerator;

/**
 * A generator of a zipfian distribution. It produces a sequence of items, such that some items are more popular than others,
 * according to a zipfian distribution. When you construct an instance of this class, you specify the number of items in the set
 * to draw from, either by specifying an itemCount (so that the sequence is of items from 0 to itemCount - 1) or by specifying a
 * min and a max (so that the sequence is of items from min to max inclusive). After you construct the instance, you can change
 * the number of items by calling {@link #nextInt()} or {@link #nextLong()}.
 * <p/>
 * Unlike @ZipfianGenerator, this class scatters the "popular" items across the item space. Use this, instead of
 * {@link ZipfianGenerator}, if you don't want the head of the distribution (the popular items) clustered together.
 */
public class ScrambledZipfianGenerator extends IntegerGenerator {
    public static final double ZETAN = 26.46902820178302;
    public static final double USED_ZIPFIAN_CONSTANT = 0.99;
    public static final long ITEM_COUNT = 10000000000L;

    private  ZipfianGenerator gen;
    private long min;
    private long max;
    private long itemCount;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     *
     * @param items The number of items in the distribution.
     */
    public ScrambledZipfianGenerator(long items) {
        this(0, items - 1);
    }

    /**
     * Create a zipfian generator for items between min and max.
     *
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     */
    public ScrambledZipfianGenerator(long min, long max) {
        this(min, max, ZipfianGenerator.ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param items The number of items in the distribution.
     * @param zipfianConstant The zipfian constant to use.
     */
    /*
    // not supported, as the value of zeta depends on the zipfian constant, and we have only precomputed zeta for one zipfian constant
	public ScrambledZipfianGenerator(long items, double zipfianConstant)
	{
		this(0, items - 1, zipfianConstant);
	}
    */

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant. If you use a
     * zipfian constant other than 0.99, this will take a long time to complete because we need to recompute zeta.
     *
     * @param min             The smallest integer to generate in the sequence.
     * @param max             The largest integer to generate in the sequence.
     * @param zipfianConstant The zipfian constant to use.
     */
    public ScrambledZipfianGenerator(long min, long max, double zipfianConstant) {
        this.min = min;
        this.max = max;
        itemCount = this.max - this.min + 1;
        if (zipfianConstant == USED_ZIPFIAN_CONSTANT) {
            gen = new ZipfianGenerator(0, ITEM_COUNT, zipfianConstant, ZETAN);
        } else {
            gen = new ZipfianGenerator(0, ITEM_COUNT, zipfianConstant);
        }
    }

    /**************************************************************************************************/

    /**
     * Return the next int in the sequence.
     */
    @Override
    public int nextInt() {
        return (int) nextLong();
    }

    /**
     * Return the next long in the sequence.
     */
    public long nextLong() {
        long ret = gen.nextLong();
        ret = min + ZipfianUtils.FNVHash64(ret) % itemCount;
        setLastInt((int) ret);
        return ret;
    }

    /**
     * Since the values are scrambled (hopefully uniformly), the mean is simply the middle of the range.
     */
    @Override
    public double mean() {
        return ((double) (min + max)) / 2.0;
    }

    public static void main(String[] args) {
        double newZetan = ZipfianGenerator.zetaStatic(ITEM_COUNT, ZipfianGenerator.ZIPFIAN_CONSTANT);
        System.out.println("zetan: " + newZetan);
        System.exit(0);

        ScrambledZipfianGenerator gen = new ScrambledZipfianGenerator(10000);

        for (int i = 0; i < 1000000; i++) {
            System.out.println(gen.nextInt());
        }
    }
}
