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

import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;


public class Q01CurrencyConversion extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline, BenchmarkProperties props) throws ValidationException {
        int eventsPerSecond = props.eventsPerSecond;
        int numDistinctKeys = props.numDistinctKeys;
        int sievingFactor = eventsPerSecond / 8192;
        StreamStage<Bid> input = pipeline
                .readFrom(eventSource("bids",
                        eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) ->
                                new Bid(seq, timestamp, seq % numDistinctKeys, getRandom(seq, 100))))
                .withNativeTimestamps(0);

        // NEXMark Query 1 start
        StreamStage<Bid> queryResult = input
                .map(bid1 -> new Bid(bid1.id(), bid1.timestamp(), bid1.auctionId(), bid1.price() * 8 / 10));
        // NEXMark Query 1 end

        return queryResult
                .filter(bid -> bid.id() % sievingFactor == 0)
                .apply(determineLatency(Bid::timestamp));
    }
}
