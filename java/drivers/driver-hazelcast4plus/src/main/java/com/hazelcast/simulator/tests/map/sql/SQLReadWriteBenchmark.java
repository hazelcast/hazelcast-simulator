package com.hazelcast.simulator.tests.map.sql;

import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.test.annotations.Verify;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.SqlClientService;

import java.util.Random;

import static org.junit.Assert.assertTrue;

public class SQLReadWriteBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataSerializablePojo> map;
    public int arraySize = 20;
    private Integer[] sampleArray;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, IdentifiedDataSerializablePojo> streamer = StreamerFactory.getInstance(map);
        sampleArray = new Integer[arraySize];
        for (int i = 0; i < arraySize; i++) {
            sampleArray[i] = i;
        }

        for (int i = 0; i < entryCount; i++) {
            Integer key = i;
            IdentifiedDataSerializablePojo value = new IdentifiedDataSerializablePojo(sampleArray, String.format("%010d", key));
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
                + "                'valueJavaClass' = 'com.hazelcast.simulator.hz.IdentifiedDataSerializablePojo'\n"
                + "        )";

        sqlService.execute(query);
    }

    @TimeStep
    public void select() throws Exception {
        SqlService sqlService = targetInstance.getSql();

        String query = "SELECT this FROM " + name + " WHERE __key = ?";
        int key = new Random().nextInt(entryCount);
        int actual = 0;
        try (SqlResult result = sqlService.execute(query, key)) {
            for (SqlRow row : result) {
                Object value = row.getObject(0);
                if (!(value instanceof IdentifiedDataSerializablePojo)) {
                    throw new IllegalStateException("Returned object is not "
                            + IdentifiedDataSerializablePojo.class.getSimpleName() + ": " + value);
                }
                actual++;
            }
        }

        if (actual != 1) {
            throw new IllegalArgumentException("Invalid count [expected=" + 1 + ", actual=" + actual + "]");
        }
    }

    @TimeStep
    public void update() throws Exception {
        SqlClientService sqlService = (SqlClientService) targetInstance.getSql();

        String query = "UPDATE " + name + " SET numbers = ?, valueField = ? WHERE __key = ?";
        Random random = new Random();
        int key = random.nextInt(entryCount);
        String value = String.format("updated%03d", random.nextInt(entryCount));
        try (SqlResult result = sqlService.execute(query, sampleArray, value, key)) {
//            if (result.updateCount() != 1) {   // result.updateCount() not implemented see https://github.com/hazelcast/hazelcast/issues/22486
//                throw new IllegalArgumentException("Invalid count [expected=" + 1 + ", actual=" + result.updateCount() + "]");
//            }
        }
    }

    /**
     * Checks that values were updated. Since we're using random keys, there's no
     * guarantee that each key will receive and update. There should be at least
     * one updated entry, but in reality there will be many. Ideally this will be
     * removed once `SqlResult.updateCount()` is implemented, and that can be used
     * for verification per-TimeStep instead.
     */
    @Verify
    public void verify() {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT * FROM " + name + " WHERE valueField LIKE 'updated%' LIMIT 1";
        try (SqlResult result = sqlService.execute(query)) {
            assertTrue(result.iterator().hasNext());
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }
}
