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
import com.hazelcast.sql.impl.client.SqlClientService;

import java.util.Random;

public class SQLReadWriteByValueBenchmark extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;
    public boolean useIndex = true;

    //16 byte + N*(20*N
    private IMap<Integer, IdentifiedDataWithLongSerializablePojo> map;
    public int arraySize = 20;
    private Integer[] sampleArray;
    private Random random;

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(name);
        random = new Random();
    }

    @Prepare(global = true)
    public void prepare() {
        if (useIndex) map.addIndex(IndexType.HASH, "value");

        Streamer<Integer, IdentifiedDataWithLongSerializablePojo> streamer = StreamerFactory.getInstance(map);
        sampleArray = generateRandomNumberArray(arraySize);

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
    public void select() throws Exception {
        SqlService sqlService = targetInstance.getSql();

        String query = "SELECT __key, this FROM " + name + " WHERE \"value\" = ?";
        int key = random.nextInt(entryCount);
        int actual = 0;
        try (SqlResult result = sqlService.execute(query, key)) {
            for (SqlRow row : result) {
                Object value = row.getObject(1);
                if (!(value instanceof IdentifiedDataWithLongSerializablePojo)) {
                    throw new IllegalStateException("Returned object is not "
                            + IdentifiedDataWithLongSerializablePojo.class.getSimpleName() + ": " + value);
                }
                actual++;
            }
        }

        if (actual != 1) {
            throw new IllegalArgumentException("Invalid count [expected=" + 1 + ", actual=" + actual + "]");
        }
    }

    @TimeStep
    public void update() {
        SqlClientService sqlService = (SqlClientService) targetInstance.getSql();

        String query = "UPDATE " + name + " SET numbers = ? WHERE \"value\" = ?";
        int value = random.nextInt(entryCount);
        try (SqlResult result = sqlService.execute(query, generateRandomNumberArray(arraySize), value)) {
            if (result.updateCount() != 0) {   // result.updateCount() not implemented see https://github.com/hazelcast/hazelcast/issues/22486
                throw new IllegalArgumentException("Invalid count [expected=" + 0 + ", actual=" + result.updateCount() + "]");
            }
        }
    }

    @Teardown
    public void tearDown() {
        map.destroy();
    }

    private Integer[] generateRandomNumberArray(int arraySize) {
        return random.ints(arraySize)
                .boxed()
                .toArray(Integer[]::new);
    }
}
