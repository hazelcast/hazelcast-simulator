package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A test that checks how fast externalizable values can be put in a map. The key is an integer.
 */
public class MapExternalizableTest {

    // properties
    public String basename = MapExternalizableTest.class.getSimpleName();
    public int threadCount = 10;
    public int keyCount = 1000000;

    // probes
    public Probe probe;

    private IMap<Integer, Value> map;

    @Setup
    public void setUp(TestContext testContext) {
        HazelcastInstance hazelcastInstance = testContext.getTargetInstance();
        map = hazelcastInstance.getMap(basename);
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {
        @Override
        protected void timeStep() throws Exception {
            int key = randomInt(keyCount);
            map.put(key, new Value(key));
        }
    }

    public static void main(String[] args) throws Exception {
        MapExternalizableTest test = new MapExternalizableTest();
        new TestRunner<MapExternalizableTest>(test).run();
    }

    public static class Value implements Externalizable {
        private int v;

        public Value() {
        }

        public Value(int v) {
            this.v = v;
        }

        public int id() {
            return v;
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            v = in.readInt();
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(v);
        }
    }
}
