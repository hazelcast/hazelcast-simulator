package com.hazelcast.simulator.tests.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.RunWithWorker;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.test.annotations.Warmup;
import com.hazelcast.simulator.worker.tasks.AbstractMonotonicWorker;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionOptions.TransactionType;
import com.hazelcast.transaction.TransactionalTask;
import com.hazelcast.transaction.TransactionalTaskContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;

public class MapTransactionContextTest {

    private static final ILogger LOGGER = Logger.getLogger(MapTransactionTest.class);

    // properties
    public String basename = getClass().getSimpleName();
    public int keyCount = 1000;
    public TransactionType transactionType = TransactionType.TWO_PHASE;
    public int durability = 1;
    public int range = 1000;

    private HazelcastInstance hz;
    private IMap<Integer, Long> map;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        hz = testContext.getTargetInstance();
        map = hz.getMap(basename);
     }

    @Warmup(global = true)
    public void warmup() throws Exception {
        for (int i = 0; i < keyCount; i++) {
            map.put(i, 0L);
        }
    }

    @RunWithWorker
    public Worker createWorker() {
        return new Worker();
    }

    private class Worker extends AbstractMonotonicWorker {

        @Override
        protected void timeStep() {
            int key = nextRandom(0, range / 2);

            TransactionOptions txOpts = new TransactionOptions().setTransactionType(transactionType).setDurability(durability);

            TransactionContext tCtx = hz.newTransactionContext(txOpts);

            tCtx.beginTransaction();

            TransactionalMap<Object, Object> txMap = tCtx.getMap("map");

            try {
                Object val = txMap.getForUpdate(key);

                if (val != null) {
                    key = nextRandom(range / 2, range);
                }

                txMap.put(key, new SampleValue(key));

                tCtx.commitTransaction();
            } catch (Exception e) {
//                System.out.println(cfg, "Yardstick transaction will be rollback.");
//
//                e.printStackTrace(cfg.error());

                e.printStackTrace();

                tCtx.rollbackTransaction();
            }
        }

        protected int nextRandom(int max) {
            return ThreadLocalRandom.current().nextInt(max);
        }
        protected int nextRandom(int min, int max) {
            return ThreadLocalRandom.current().nextInt(max - min) + min;
        }
    }

    /**
     * Entity class for benchmark.
     */
    public static class SampleValue implements Externalizable {
        /** */
        private int id;

        /** */
        public SampleValue() {
            // No-op.
        }

        /**
         * @param id Id.
         */
        public SampleValue(int id) {
            this.id = id;
        }

        /**
         * @return Id.
         */
        public int id() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            id = in.readInt();
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeInt(id);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "Value [id=" + id + ']';
        }
    }


    /*
   @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        int key = nextRandom(0, args.range() / 2);

        // Repeatable read isolation level is always used.

    }
     */
}
