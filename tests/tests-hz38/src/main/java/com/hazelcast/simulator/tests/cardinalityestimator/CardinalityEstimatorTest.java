/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.cardinalityestimator;

import static com.hazelcast.simulator.tests.helpers.KeyLocality.SHARED;
import static com.hazelcast.simulator.tests.helpers.KeyUtils.generateStringKeys;
import static org.junit.Assert.assertEquals;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.tests.helpers.KeyLocality;

public class CardinalityEstimatorTest extends AbstractTest {

    // properties
    public KeyLocality keyLocality = SHARED;
    public int countersLength = 1000;

    private CardinalityEstimator totalCounter;
    private String[] elements;

    @Setup
    public void setup() {
        totalCounter = targetInstance.getCardinalityEstimator(name + ":TotalCounter");
   
        elements = generateStringKeys(name, countersLength, keyLocality, targetInstance);
        for (int i = 0; i < countersLength; i++) {
        	totalCounter.add(elements[i]);
        }
    }

    @TimeStep(prob = 1)
    public void write(ThreadState state) {
        totalCounter.add(elements[state.randomCounter()]);
    }

    public class ThreadState extends BaseThreadState {

        private int randomCounter() {
            int index = randomInt(countersLength);
            return index;
        }
    }


    @Verify
    public void verify() {
        assertEquals(totalCounter.estimate(), countersLength);
    }

    @Teardown
    public void teardown() {
        totalCounter.destroy();
    }
}
