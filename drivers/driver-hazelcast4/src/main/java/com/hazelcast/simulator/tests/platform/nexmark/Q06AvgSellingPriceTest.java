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

import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.platform.nexmark.model.Auction;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import java.util.ArrayDeque;
import java.util.OptionalDouble;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.EventSourceP.eventSource;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.JoinAuctionToWinningBidP.joinAuctionToWinningBid;
import static java.lang.Math.max;

public class Q06AvgSellingPriceTest extends BenchmarkBase {
    // properties
    public int eventsPerSecond = 100_000;
    public int numDistinctKeys = 1_000;
    public String pgString = "none"; // none, at-least-once or exactly-once:
    public long snapshotIntervalMillis = 1_000;
    public int warmupSeconds = 5;
    public int measurementSeconds = 55;
    public int latencyReportingThresholdMs = 10;

    private Job job;

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(Pipeline pipeline) throws ValidationException {
        int bidsPerSecond = this.eventsPerSecond;
        int numDistinctKeys = this.numDistinctKeys;
        int auctionsPerSecond = 1000;
        int bidsPerAuction = max(1, bidsPerSecond / auctionsPerSecond);
        long auctionMinDuration = (long) numDistinctKeys * bidsPerAuction * 1000 / bidsPerSecond;
        long auctionMaxDuration = 2 * auctionMinDuration;
        System.out.format("Auction duration: %,d .. %,d ms%n", auctionMinDuration, auctionMaxDuration);
        long maxBid = 1000;
        int windowItemCount = 10;

        // We generate auctions at rate eventsPerSecond / bidsPerAuction.
        // We generate bids at rate eventsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .<Object>readFrom(eventSource("auctions", bidsPerSecond / bidsPerAuction, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long sellerId = getRandom(137 * seq, numDistinctKeys);
                            long duration = auctionMinDuration
                                    + getRandom(271 * seq, auctionMaxDuration - auctionMinDuration);
                            int category = (int) getRandom(743 * seq, 128);
                            return new Auction(seq, timestamp, sellerId, category, timestamp + duration);
                        }))
                .withNativeTimestamps(0);

        StreamStage<Bid> bids = pipeline
                .readFrom(eventSource("bids", bidsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long price = getRandom(seq, maxBid);
                            long auctionId = seq / bidsPerAuction;
                            return new Bid(seq, timestamp, auctionId, price);
                        }))
                .withNativeTimestamps(0);

        // NEXMark Query 6 start
        StreamStage<Tuple2<OptionalDouble, Long>> queryResult = auctions
                .merge(bids)
                .apply(joinAuctionToWinningBid(auctionMaxDuration))
                .groupingKey(auctionAndBid -> auctionAndBid.f0().sellerId())
                .mapStateful(() -> new ArrayDeque<Long>(windowItemCount),
                        (deque, key, item) -> {
                            if (deque.size() == windowItemCount) {
                                deque.removeFirst();
                            }
                            deque.addLast(item.f1().price());
                            return tuple2(deque.stream().mapToLong(i -> i).average(), item.f0().expires());
                        });
        // NEXMark Query 6 end

        // queryResult: Tuple2(averagePrice, auctionExpirationTime)
        return queryResult.apply(determineLatency(Tuple2::f1));
    }

    @Prepare(global = true)
    public void prepareJetJob() {
        ProcessingGuarantee guarantee = ProcessingGuarantee.valueOf(pgString.toUpperCase().replace('-', '_'));
        BenchmarkProperties props = new BenchmarkProperties(
                eventsPerSecond,
                numDistinctKeys,
                guarantee,
                snapshotIntervalMillis,
                warmupSeconds,
                measurementSeconds,
                latencyReportingThresholdMs
        );
        job = this.run(targetInstance, props);
    }

    @Run
    public void doNothing() {
    }

    @Teardown(global = true)
    public void tearDownJetJob() {
        job.cancel();
    }
}
