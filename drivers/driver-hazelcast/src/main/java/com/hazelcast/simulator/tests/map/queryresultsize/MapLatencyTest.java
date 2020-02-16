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
package com.hazelcast.simulator.tests.map.queryresultsize;

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;

/**
 * This test creates latency probe results for {@link com.hazelcast.map.IMap#values()}, {@link com.hazelcast.map.IMap#keySet()}
 * and {@link com.hazelcast.map.IMap#entrySet()}. It is used to ensure that the query result size limit has no bad impact on the
 * latency of those method calls.
 *
 * To achieve this we fill a map slightly below the trigger limit of the query result size limit, so we are sure it will never
 * trigger the exception. Then we call the map methods and measure their latency.
 *
 * The test can be configured to use {@link String} or {@link Integer} keys. You can also override the number of filled items
 * with the {@link #keyCount} property, e.g. to test the latency impact of this feature with an empty map.
 *
 * This test works fine with all Hazelcast versions, since it does not use any new functionality. Just be sure the default
 * values in {@link AbstractMapTest} match with the default values of the query result size limit in Hazelcast 3.5. Otherwise
 * the map will be filled with a different number of keys and the latency results may not be comparable.
 */
public class MapLatencyTest extends AbstractMapTest {

    // properties
    public String keyType = "String";
    public int keyCount = -1;

    @Setup
    public void setUp() {
        baseSetup();
    }

    @Override
    long getGlobalKeyCount(Integer minResultSizeLimit, Float resultLimitFactor) {
        if (keyCount > -1) {
            return keyCount;
        }
        return Math.round(minResultSizeLimit * resultLimitFactor * 0.9);
    }

    @Prepare
    public void prepare() {
        basePrepare(keyType);
    }

    @Verify
    public void globalVerify() {
        baseVerify(false);
    }
}
