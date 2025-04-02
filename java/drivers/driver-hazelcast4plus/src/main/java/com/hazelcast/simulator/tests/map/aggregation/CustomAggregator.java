/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.map.aggregation;

import com.hazelcast.aggregation.Aggregator;

import java.util.Map;

// TODO: IDS
public class CustomAggregator<K> implements Aggregator<Map.Entry<K, DoubleCompactPojo>, DoubleSerializablePojo> {

    private DoubleSerializablePojo result = new DoubleSerializablePojo();

    @Override
    public void accumulate(Map.Entry<K, DoubleCompactPojo> input) {
        result.n1 += input.getValue().n1;
        result.n2 += input.getValue().n2;
        result.n3 += input.getValue().n3;
        result.n4 += input.getValue().n4;
    }

    @Override
    public void combine(Aggregator aggregator) {
        CustomAggregator<K> customAggregator = (CustomAggregator<K>) aggregator;
        result.n1 += customAggregator.result.n1;
        result.n2 += customAggregator.result.n2;
        result.n3 += customAggregator.result.n3;
        result.n4 += customAggregator.result.n4;
    }

    @Override
    public DoubleSerializablePojo aggregate() {
        // TODO: returns serializable variant to avoid registering compact serializer on client side
//            return new DoubleCompactPojo(result.n1, result.n2, result.n3, result.n4);
        return result;
    }
}
