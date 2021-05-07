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
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.platform.nexmark.model.Auction;
import com.hazelcast.simulator.tests.platform.nexmark.model.Bid;

import static com.hazelcast.function.ComparatorEx.comparingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.maxBy;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.EventSourceP.eventSource;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.JoinAuctionToWinningBidP.joinAuctionToWinningBid;
import static java.lang.Math.max;
import static java.lang.Math.min;

public class Q04AveragePriceForCategoryTest extends BenchmarkBase {

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
        int bidsPerSecond = eventsPerSecond;
        int auctionsPerSecond = min(1000, max(1000, bidsPerSecond));
        int bidsPerAuction = bidsPerSecond / auctionsPerSecond;

        // Mean duration of the auctions determines the number of keys stored
        // in the joining stage (joinAuctionToWinningBid). We randomize the
        // duration between 1/2 and 3/2 of the requested number so the average
        // comes out to 2.
        long auctionMinDuration = numDistinctKeys * 1000L / auctionsPerSecond / 2;
        long auctionMaxDuration = 3 * auctionMinDuration;
        System.out.format("Auction duration: %,d .. %,d ms%n", auctionMinDuration, auctionMaxDuration);

        // We generate auctions at rate bidsPerSecond / bidsPerAuction.
        // We generate bids at rate bidsPerSecond, each bid refers to
        // auctionId = seq / bidsPerAuction

        StreamStage<Object> auctions = pipeline
                .<Object>readFrom(eventSource("auctions", auctionsPerSecond, INITIAL_SOURCE_DELAY_MILLIS,
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
                        (seq, timestamp) -> new Bid(seq, timestamp, seq / bidsPerAuction, 0)))
                .withNativeTimestamps(0);

        // NEXMark Query 4 start
        StreamStage<Tuple3<Integer, Double, Long>> queryResult = auctions
                .merge(bids)
                .apply(joinAuctionToWinningBid(auctionMaxDuration)) // Tuple2(auction, winningBid)
                .map(t -> tuple3(t.f0().category(), t.f1().price(), t.f0().expires())) // "catPriceExpires"
                .groupingKey(Tuple3::f0)
                .rollingAggregate(allOf(averagingLong(Tuple3::f1), maxBy(comparingLong(Tuple3::f2))))
                .map(catAndAggrResults -> {
                    int category = catAndAggrResults.getKey();
                    Tuple2<Double, Tuple3<Integer, Long, Long>> aggrResults = catAndAggrResults.getValue();
                    Tuple3<Integer, Long, Long> catPriceExpires = aggrResults.f1();
                    double averagePrice = aggrResults.f0();
                    long latestAuctionEnd = catPriceExpires.f2();
                    return tuple3(category, averagePrice, latestAuctionEnd);
                });
        // NEXMark Query 4 end

        // queryResult: Tuple3(category, averagePrice, latestAuctionEnd)
        return queryResult.apply(determineLatency(Tuple3::f2));
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
