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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// TODO: IDS
public class AllOfAggregator<K, V> implements Aggregator<Map.Entry<K, V>, List<Object>> {

    private List<Aggregator<Map.Entry<K, V>, ? extends Object>> aggregators = new ArrayList<>();

    AllOfAggregator(List<Aggregator<Map.Entry<K, V>, ? extends Object>> aggregators) {
        this.aggregators = aggregators;
    }

    AllOfAggregator() {
    }

    AllOfAggregator<K, V> add(Aggregator<Map.Entry<K, V>, ? extends Object> aggregator) {
        aggregators.add(aggregator);
        return this;
    }

    @Override
    public void accumulate(Map.Entry<K, V> input) {
        aggregators.forEach(a -> a.accumulate(input));
    }

    @Override
    public void onAccumulationFinished() {
        aggregators.forEach(Aggregator::onAccumulationFinished);
    }

    @Override
    public void combine(Aggregator other) {
        if (!(other instanceof AllOfAggregator<?, ?> otherAllOf)) {
            throw new IllegalArgumentException("Only AllOfAggregator should be combined");
        }
        for (int i = 0; i < aggregators.size(); i++) {
            aggregators.get(i).combine(otherAllOf.aggregators.get(i));
        }
    }

    @Override
    public void onCombinationFinished() {
        aggregators.forEach(Aggregator::onCombinationFinished);
    }

    @Override
    public List<Object> aggregate() {
        List<Object> result = new ArrayList<>(aggregators.size());
        aggregators.forEach(a -> result.add(a.aggregate()));
        return result;
    }
}
