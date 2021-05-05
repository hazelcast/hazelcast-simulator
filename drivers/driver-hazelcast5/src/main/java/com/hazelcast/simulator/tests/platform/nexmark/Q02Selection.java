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
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import java.util.Properties;

import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;

public class Q02Selection extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int auctionIdModulus = 128;
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int sievingFactor = Math.max(1, eventsPerSecond / (8192 * auctionIdModulus));

        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource("bids", eventsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) ->
                        new Bid(seq, timestamp, seq % numDistinctKeys, getRandom(seq, 100))))
                .withNativeTimestamps(0);

        // NEXMark Query 2 start
        StreamStage<Tuple3<Long, Long, Long>> queryResult = bids
                .filter(bid -> bid.auctionId() % auctionIdModulus == 0)
                .map(bid -> tuple3(bid.timestamp(), bid.auctionId(), bid.price()));
        // NEXMark Query 2 end

        return queryResult
                .filter(t -> t.f1() % sievingFactor == 0)
                .apply(determineLatency(Tuple3::f0));
    }
}
