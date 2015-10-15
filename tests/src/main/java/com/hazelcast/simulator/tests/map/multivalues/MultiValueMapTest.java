package com.hazelcast.simulator.tests.map.multivalues;

import com.hazelcast.core.IMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.utils.ThrottlingLogger;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.simulator.worker.selector.OperationSelectorBuilder;
import com.hazelcast.simulator.worker.tasks.AbstractWorker;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.logging.Level;

import static java.lang.Math.abs;
import static org.junit.Assert.assertEquals;


public class MultiValueMapTest {
    private static final ILogger LOGGER = Logger.getLogger(MultiValueMapTest.class);
    private static final ThrottlingLogger THROTTLING_LOGGER = ThrottlingLogger.newLogger(LOGGER, 5000);


    private OperationSelectorBuilder<Operation> operationSelectorBuilder;

    private enum Operation {
        PUT,
        QUERY,
    }

    private IMap<Integer, SillySequence> map;


    public String basename = MultiValueMapTest.class.getSimpleName();
    public boolean useIndex;
    public int keyCount = 100000;
    public int maxNestedValues = 100;
    public double putProbability = 0.5;

    @Setup
    public void setUp(TestContext testContext) throws Exception {
        map = testContext.getTargetInstance().getMap(basename + "-" + testContext.getTestId());

        operationSelectorBuilder = new OperationSelectorBuilder<Operation>()
                .addOperation(Operation.PUT, putProbability)
                .addDefaultOperation(Operation.QUERY);
    }


    @Warmup(global = true)
    public void warmup() throws InterruptedException {
        if (useIndex) {
            map.addIndex("payloadField[*]", true);
        }
        loadInitialData();
    }

    private void loadInitialData() {
        Streamer<Integer, SillySequence> streamer = StreamerFactory.getInstance(map);
        for (int i = 0; i < keyCount; i++) {
            int count = i % maxNestedValues;
            SillySequence sillySequence = new SillySequence(i, count);
            streamer.pushEntry(i, sillySequence);
        }
        streamer.await();
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker(operationSelectorBuilder);
    }

    private class Worker extends AbstractWorker<Operation> {
        public Worker(OperationSelectorBuilder<Operation> operationSelectorBuilder) {
            super(operationSelectorBuilder);
        }

        private int getRandomKey() {
            return abs(randomInt(keyCount));
        }

        @Override
        protected void timeStep(Operation operation) throws Exception {
            switch (operation) {
                case PUT: {
                    int key = getRandomKey();
                    int count = key % maxNestedValues;
                    SillySequence sillySequence = new SillySequence(key, count);
                    map.put(key, sillySequence);
                    break;
                }
                case QUERY: {
                    int key = getRandomKey();
                    Predicate predicate = Predicates.equal("payloadField[*]", key);
                    Collection<SillySequence> result = map.values(predicate);
                    THROTTLING_LOGGER.log(Level.INFO, "Query 'payloadField[*]= " + key + "' returned " + result.size() + " results.");
                    for (SillySequence sillySequence : result) {
                        assertValidSequence(sillySequence);
                    }
                }
            }
        }

        private void assertValidSequence(SillySequence sillySequence) {
            Collection<Integer> payload = sillySequence.getPayload();
            assertEquals(sillySequence.getCount(), payload.size());

            Integer lastValue = null;
            for (int i : payload) {
                if (lastValue != null) {
                    int expectedValue = lastValue + 1;
                    assertEquals(expectedValue, i);
                }
                lastValue = i;
            }
        }
    }

    static class SillySequence implements DataSerializable {
        int count;
        Collection<Integer> payloadField;

        SillySequence() {

        }

        SillySequence(int from, int count) {
            this.count = count;
            this.payloadField = new ArrayList<Integer>(count);

            int to = from + count;
            for (int i = from; i < to; i++) {
                payloadField.add(i);
            }
        }

        public Collection<Integer> getPayload() {
            return payloadField;
        }

        public int getCount() {
            return count;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(count);
            out.writeObject(payloadField);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            count = in.readInt();
            payloadField = in.readObject();
        }
    }

}
