package com.hazelcast.simulator.tests.map.predicate;

import com.hazelcast.map.IMap;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Map;
import java.util.Random;
import java.util.Set;

public class PartitionPredicateKey1EntryBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataSerializablePojo> map;
    private final int arraySize = 20;
    private Random random;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        this.random = new Random();
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, IdentifiedDataSerializablePojo> streamer = StreamerFactory.getInstance(map);
        Integer[] sampleArray = new Integer[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sampleArray[i] = i;
        }

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            IdentifiedDataSerializablePojo value = new IdentifiedDataSerializablePojo(sampleArray, String.format("%010d", key));
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep() throws Exception {
        int key = random.nextInt(entryCount);
        Predicate<Integer, IdentifiedDataSerializablePojo> partitionPredicate =
                Predicates.partitionPredicate(key, Predicates.sql("__key = " + key));
        Set<Map.Entry<Integer, IdentifiedDataSerializablePojo>> entries = map.entrySet(partitionPredicate);
        if (entries.size() != 1) {
            throw new Exception("wrong entry count");
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
