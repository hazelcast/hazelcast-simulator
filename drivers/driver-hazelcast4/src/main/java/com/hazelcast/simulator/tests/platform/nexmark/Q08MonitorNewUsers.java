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

package com.hazelcast.simulator.tests.platform.nexmark;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.tests.platform.nexmark.model.Auction;
import com.hazelcast.simulator.tests.platform.nexmark.model.Person;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;

public class Q08MonitorNewUsers extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, BenchmarkProperties props
    ) throws ValidationException {
        int eventsPerSecond = props.eventsPerSecond;
        int numDistinctKeys = props.numDistinctKeys;
        int windowSize = props.windowSize;
        long slideBy = props.slideBy;
        int sievingFactor = numDistinctKeys / 100;

        StreamStage<Person> persons = pipeline
                .readFrom(eventSource("sellers", eventsPerSecond / 10, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long id = getRandom(seq, numDistinctKeys);
                            return new Person(id, timestamp, "Seller #" + id, null);
                        }))
                .withNativeTimestamps(0);

        StreamStage<Auction> auctions = pipeline
                .readFrom(eventSource("auctions", eventsPerSecond * 9L / 10, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            // factor 137 removes the correlation between these IDs and those in the Person stream
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            return new Auction(0, timestamp, sellerId, 0, 0);
                        }))
                .withNativeTimestamps(0);

        // NEXMark Query 8 start
        StreamStage<KeyedWindowResult<Long, Tuple2<Person, Long>>> queryResult = persons
                .window(sliding(windowSize, slideBy))
                .groupingKey(Person::id)
                .aggregate2(pickAny(), auctions.groupingKey(Auction::sellerId), counting())
                .filter(kwr -> kwr.result().f0() != null && kwr.result().f1() > 0);
        // NEXMark Query 8 end

        return queryResult
                .filter(kwr -> kwr.key() % sievingFactor == 0)
                .apply(determineLatency(WindowResult::end));
    }

    @SuppressWarnings("checkstyle")
    public static <T> AggregateOperation1<T, PickAnyAccumulator<T>, T> pickAny() {
        return AggregateOperation
                .withCreate(() -> new PickAnyAccumulator<T>())
                .<T>andAccumulate(PickAnyAccumulator::accumulate)
                .andCombine(PickAnyAccumulator::combine)
                .andDeduct(PickAnyAccumulator::deduct)
                .andExportFinish(PickAnyAccumulator::get);
    }

}
