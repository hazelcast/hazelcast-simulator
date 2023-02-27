package com.hazelcast.simulator.tests.map.sql;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataWithLongSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Random;

public class ScanByRangeBenchmark extends HazelcastTest {

    public int entryCount = 10_000_000;
    public boolean useIndex = true;
    public int rangeSize = 10_000;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataWithLongSerializablePojo> map;
    public int arraySize = 20;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        if (useIndex) map.addIndex(IndexType.SORTED, "value");

        Streamer<Integer, IdentifiedDataWithLongSerializablePojo> streamer = StreamerFactory.getInstance(map);
        Integer[] sampleArray = new Integer[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sampleArray[i] = i;
        }

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            IdentifiedDataWithLongSerializablePojo value = new IdentifiedDataWithLongSerializablePojo(sampleArray, key.longValue());
            streamer.pushEntry(key, value);
        }
        streamer.await();

        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + name + " "
                + "EXTERNAL NAME " + name + " "
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.Integer',\n"
                + "                'valueFormat' = 'java',\n"
                + "                'valueJavaClass' = 'com.hazelcast.simulator.hz.IdentifiedDataWithLongSerializablePojo'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void timeStep() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT __key, this FROM " + name + " WHERE \"value\" BETWEEN ? AND ? ";
        Random random = new Random();
        int min = random.nextInt(entryCount);
        int max = Integer.min(min + rangeSize, entryCount - 1);
        long actual = 0L;
        try (SqlResult result = sqlService.execute(query, min, max)) {
            for (SqlRow row : result) {
                Object value = row.getObject(1);
                if (!(value instanceof IdentifiedDataWithLongSerializablePojo)) {
                    throw new IllegalStateException("Returned object is not "
                            + IdentifiedDataWithLongSerializablePojo.class.getSimpleName() + ": " + value);
                }
                actual++;
            }
        }

        int expected = max - min + 1;
        if (actual != expected) {
            throw new IllegalArgumentException("Invalid count [expected=" + expected + ", actual=" + actual + "]");
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
