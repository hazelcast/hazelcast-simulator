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
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple5;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.tests.platform.nexmark.model.Auction;
import com.hazelcast.simulator.tests.platform.nexmark.model.Person;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.jet.Traversers.empty;
import static com.hazelcast.jet.Traversers.singleton;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.datamodel.Tuple5.tuple5;
import static com.hazelcast.simulator.tests.platform.nexmark.processor.EventSourceP.eventSource;

public class Q03LocalItemSuggestionTest extends BenchmarkBase {

    private static final String[] STATES = {
            "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA",
            "ME", "MD", "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
            "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
    };

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
        // This code respects numDistinctKeys indirectly, by creating a pattern of
        // seller IDs over time so that there are auctions with a given seller ID
        // for a limited time. A "cloud" of random seller IDs slowly moves up the
        // integer line. It advances by one every auctionsPerSeller. The width
        // of the cloud is numDistinckKeys. We calculate the TTL for the keyed
        // mapStateful stage to match the amount of time during which this cloud
        // covers any given seller ID.
        int auctionsPerSecond = this.eventsPerSecond;
        int auctionsPerSeller = 100;
        int numDistinctKeys = this.numDistinctKeys;
        long ttl = (long) numDistinctKeys * auctionsPerSeller * 1000 / auctionsPerSecond;

        StreamStage<Object> sellers = pipeline
                .readFrom(eventSource("sellers", auctionsPerSecond / auctionsPerSeller, INITIAL_SOURCE_DELAY_MILLIS,
                        (seq, timestamp) -> {
                            long id = seq;
                            return new Person(id, timestamp, "Seller #" + id, STATES[seq.intValue() % STATES.length]);
                        }))
                .withNativeTimestamps(0)
                .filter(p  -> p.state().equals("OR") || p.state().equals("CA") || p.state().equals("ID"))
                .map(p -> p); // upcast

        StreamStage<Auction> auctions = pipeline
                .readFrom(eventSource("auctions", auctionsPerSecond, INITIAL_SOURCE_DELAY_MILLIS, (seq, timestamp) -> {
                    long sellerId = seq / auctionsPerSeller - getRandom(seq, numDistinctKeys);
                    if (sellerId < 0) {
                        return new Auction(seq, timestamp, 0, 1, 0); // will be filtered out
                    }
                    int categoryId = (int) getRandom(seq, 10);
                    return new Auction(seq, timestamp, sellerId, categoryId, timestamp + ttl);
                }))
                .withNativeTimestamps(0)
                .filter(a -> a.category() == 0);

        // NEXMark Query 3 start
        StreamStage<Tuple5<String, String, Long, Long, Integer>> queryResult = sellers
                .merge(auctions)
                .groupingKey(o -> o instanceof Person ? ((Person) o).id() : ((Auction) o).sellerId())
                .flatMapStateful(ttl, JoinAuctionToSeller::new, Q03LocalItemSuggestionTest.JoinAuctionToSeller::flatMap,
                        (state, key, wm) -> empty());
        // NEXMark Query 3 end

        // queryResult: Tuple5(sellerName, sellerState, auctionStart, auctionId, auctionCategory)
        return queryResult.apply(determineLatency(Tuple5::f2));
    }

    private static final class JoinAuctionToSeller implements Serializable {
        Person person;
        final List<Auction> auctions = new ArrayList<>();

        public Traverser<Tuple5<String, String, Long, Long, Integer>> flatMap(Long key, Object o) {
            if (o instanceof Person) {
                Person person = (Person) o;
                this.person = person;
                return traverseIterable(auctions)
                        .map(auction -> tuple5(person.name(), person.state(), auction.timestamp(), auction.id(),
                                auction.category()));
            } else {
                Auction auction = (Auction) o;
                auctions.add(auction);
                return person != null
                        ? singleton(tuple5(
                        person.name(), person.state(), auction.timestamp(), auction.id(), auction.category()))
                        : empty();
            }
        }
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

