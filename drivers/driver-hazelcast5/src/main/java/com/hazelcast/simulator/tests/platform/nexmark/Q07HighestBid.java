/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.jet.datamodel.WindowResult;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import java.util.Properties;

import static com.hazelcast.function.ComparatorEx.comparing;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;
import static java.lang.Math.max;

public class Q07HighestBid extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int tumblingWindowSizeMillis = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);

        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource("bids", eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> new Bid(seq, timestamp, seq, getRandom(seq, max(1, seq / 100)))))
                .withNativeTimestamps(0);

        // NEXMark Query 7 start
        StreamStage<WindowResult<Bid>> queryResult = bids
                .window(tumbling(tumblingWindowSizeMillis))
                .aggregate(maxBy(comparing(Bid::price)));
        // NEXMark Query 7 end

        return queryResult.apply(determineLatency(WindowResult::end));
    }
}
