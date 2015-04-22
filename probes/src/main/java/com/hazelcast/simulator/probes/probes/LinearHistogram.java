/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.HistogramPart;

import java.io.Serializable;
import java.util.Arrays;

import static java.lang.String.format;

public class LinearHistogram implements Serializable {

    private final int maxValue;
    private final int step;
    private final int[] buckets;

    public LinearHistogram(int maxValue, int step) {
        this(maxValue, step, null);
    }

    private LinearHistogram(int maxValue, int step, int[] buckets) {
        if (maxValue <= 0) {
            throw new IllegalArgumentException("Maximum value must be great then 0. Passed maximum value: " + maxValue);
        }
        if (step <= 0) {
            throw new IllegalArgumentException("Step must be great then 0. Passed step: " + step);
        }
        this.maxValue = maxValue;
        this.step = step;
        this.buckets = (buckets == null ? createBuckets(maxValue) : buckets);
    }

    public void addValue(int value) {
        if (value < 0) {
            throw new IllegalArgumentException("Value cannot be a negative number. Passed value: " + value);
        }
        int bucket = calculateBucket(value);
        buckets[bucket]++;
    }

    public void addMultipleValues(int value, int times) {
        if (value < 0) {
            throw new IllegalArgumentException("Value cannot be a negative number. Passed value: " + value);
        }
        int bucket = calculateBucket(value);
        buckets[bucket] += times;
    }

    public HistogramPart getPercentile(double percentile) {
        int[] copyOfBuckets = getBuckets();
        int noOfValues = getNoOfValues(copyOfBuckets);
        int minNoOfValues = (int) (percentile * noOfValues);

        int values = 0;
        for (int i = 0; i < copyOfBuckets.length - 1; i++) {
            int valuesInBucket = copyOfBuckets[i];
            values += valuesInBucket;
            if (values >= minNoOfValues) {
                int bucket = (i + 1) * step;
                return new HistogramPart(bucket, values);
            }
        }
        return new HistogramPart(maxValue, noOfValues);
    }

    public int[] getBuckets() {
        int noOfBuckets = buckets.length;
        int[] copy = new int[noOfBuckets];
        System.arraycopy(buckets, 0, copy, 0, noOfBuckets);
        return copy;
    }

    public int getMaxValue() {
        return maxValue;
    }

    public int getStep() {
        return step;
    }

    public LinearHistogram combine(LinearHistogram other) {
        validateBeforeCombining(other);
        int noOfBuckets = buckets.length;
        int[] combinedBuckets = new int[noOfBuckets];
        for (int i = 0; i < noOfBuckets; i++) {
            combinedBuckets[i] = buckets[i] + other.buckets[i];
        }
        return new LinearHistogram(maxValue, step, combinedBuckets);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        LinearHistogram that = (LinearHistogram) o;

        if (maxValue != that.maxValue) {
            return false;
        }
        if (step != that.step) {
            return false;
        }
        if (!Arrays.equals(buckets, that.buckets)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = maxValue;
        result = 31 * result + step;
        result = 31 * result + Arrays.hashCode(buckets);
        return result;
    }

    private int getNoOfValues(int[] copyOfBuckets) {
        int noOfValues = 0;
        for (int noOfValuesInBucket : copyOfBuckets) {
            noOfValues += noOfValuesInBucket;
        }
        return noOfValues;
    }

    private int[] createBuckets(int maxValue) {
        int noOfBuckets = calculateBucket(maxValue) + 1;
        return new int[noOfBuckets + 1];
    }

    private int calculateBucket(int value) {
        if (value > maxValue) {
            return buckets.length - 1;
        } else {
            return (value / step);
        }
    }

    private void validateBeforeCombining(LinearHistogram other) {
        if (maxValue != other.maxValue) {
            throw new IllegalStateException(format(
                    "Cannot combine other %s with %s as this other has max value set to %d and the other has max value set to %d",
                    this, other, maxValue, other.maxValue));
        }
        if (step != other.step) {
            throw new IllegalStateException(format(
                    "Cannot combine other %s with %s as this other has step set to %d and the other has step set to %d",
                    this, other, step, other.step));
        }
    }
}
