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

import java.util.Properties;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.simulator.tests.platform.nexmark.EventSourceP.eventSource;

public class Q08MonitorNewUsers extends BenchmarkBase {

    @Override
    StreamStage<Tuple2<Long, Long>> addComputation(
            Pipeline pipeline, Properties props
    ) throws ValidationException {
        int eventsPerSecond = parseIntProp(props, PROP_EVENTS_PER_SECOND);
        int numDistinctKeys = parseIntProp(props, PROP_NUM_DISTINCT_KEYS);
        int windowSize = parseIntProp(props, PROP_WINDOW_SIZE_MILLIS);
        long slideBy = parseIntProp(props, PROP_SLIDING_STEP_MILLIS);
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

    public static <T> AggregateOperation1<T, PickAnyAccumulator<T>, T> pickAny() {
        return AggregateOperation
                .withCreate(PickAnyAccumulator<T>::new)
                .<T>andAccumulate(PickAnyAccumulator::accumulate)
                .andCombine(PickAnyAccumulator::combine)
                .andDeduct(PickAnyAccumulator::deduct)
                .andExportFinish(PickAnyAccumulator::get);
    }

}
