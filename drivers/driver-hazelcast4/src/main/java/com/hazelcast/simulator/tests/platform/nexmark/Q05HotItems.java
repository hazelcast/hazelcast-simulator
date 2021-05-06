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

import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import java.util.List;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;

public class Q05HotItems extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, BenchmarkProperties props
    ) throws ValidationException {
        int eventsPerSecond = props.eventsPerSecond;
        int numDistinctKeys = props.numDistinctKeys;
        int windowSize = props.windowSize;
        long slideBy = props.slideBy;
        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource("bids", eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) ->
                        new Bid(seq, timestamp, seq % numDistinctKeys, 0)))
                .withNativeTimestamps(0);

        // NEXMark Query 5 start
        StreamStage<WindowResult<List<KeyedWindowResult<Long, Long>>>> queryResult = bids
                .window(sliding(windowSize, slideBy))
                .groupingKey(Bid::auctionId)
                .aggregate(counting())
                .window(tumbling(slideBy))
                .aggregate(topN(10, comparing(KeyedWindowResult::result)));
        // NEXMark Query 5 end

        return queryResult.apply(determineLatency(WindowResult::end));
    }
}

