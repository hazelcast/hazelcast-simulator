package com.hazelcast.simulator.tests.map;

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;

import java.util.Random;

public class MapPutEntryIdentifiedDataSerializableBenchmark extends HazelcastTest {
    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataSerializablePojo> map;
    private Integer[] sampleArray;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        int arraySize = 20;
        this.sampleArray = new Integer[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sampleArray[i] = i;
        }
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, IdentifiedDataSerializablePojo> streamer = StreamerFactory.getInstance(map);

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            IdentifiedDataSerializablePojo value = new IdentifiedDataSerializablePojo(sampleArray, String.format("%010d", key));
            streamer.pushEntry(key, value);
        }
        streamer.await();
    }

    @TimeStep
    public void timeStep() throws Exception {
        Random random = new Random();
        int key = random.nextInt(entryCount);
        IdentifiedDataSerializablePojo value = new IdentifiedDataSerializablePojo(sampleArray, String.format("%010d", random.nextInt(entryCount)));
        IdentifiedDataSerializablePojo serializablePojo = map.put(key, value);
        assert serializablePojo != null;
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
