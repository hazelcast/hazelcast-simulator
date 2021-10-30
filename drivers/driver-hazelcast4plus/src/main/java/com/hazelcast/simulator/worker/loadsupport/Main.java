package com.hazelcast.simulator.worker.loadsupport;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;

public class Main {
    public static void main(String[] args) throws Exception {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        // we ingest into a <Long,Long> IMap
        IMapGeneratingIngestor.<Long, Long>configure()
                // coordinated by instance hz
                .withHazelcastInstance(hz)
                // into map 'mymap'
                .withMapName("mymap")
                // ingest by sending tasks to all members, each task on every member to put 1000 generated entries
                .withBatchSize(1000)
                // each tasks run 10 async sets concurrently
                .withThrottle(100)
                // each member runs the ingestion tasks on 5 threads, in HZ executor
                .withParallelism(5)
                // once ingestion is done, there will be 69352 entries in the map
                .withTotalEntries(69352)
                // the keys are generated based on a sequence, this time with an identity function
                .withKeyGenerator(seqAsKey -> seqAsKey)
                // the values are generated based on the key, this time constant 42
                .withValueGenerator(key -> 42L)
                // ok, let's do it...
                .ingest();
        // done
        hz.getLoggingService().getLogger(Main.class).info("Map size: " + hz.getMap("mymap").size());
    }
}
