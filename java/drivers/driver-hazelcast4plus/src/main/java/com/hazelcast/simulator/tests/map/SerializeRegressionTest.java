package com.hazelcast.simulator.tests.map;

import com.hazelcast.config.IndexType;
import com.hazelcast.map.IMap;
import com.hazelcast.simulator.hz.HazelcastTest;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.worker.loadsupport.Streamer;
import com.hazelcast.simulator.worker.loadsupport.StreamerFactory;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlRow;
import com.hazelcast.sql.SqlService;

import java.util.Random;

public class SerializeRegressionTest extends HazelcastTest {
    // properties
    public int itemCount = 1_000_000;
    public String query;

    private IMap<Long, RegressionObject> map;

    @Setup
    public void setUp() {
        map = targetInstance.getMap(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        Streamer<Long, RegressionObject> streamer = StreamerFactory.getInstance(map);
        for (long i = 0; i < itemCount; i++) {
            streamer.pushEntry(i, new RegressionObject(random.nextDouble(), random.nextDouble()));
        }
        streamer.await();

        map.addIndex(IndexType.SORTED, "doubleValue1");
        map.addIndex(IndexType.SORTED, "doubleValue2");
        map.addIndex(IndexType.SORTED, "doubleValue3");
        System.out.println("Map created");

        String query = "CREATE MAPPING " + name + " " +
                "        TYPE IMap\n" +
                "        OPTIONS (\n" +
                "  'keyFormat' = 'java',\n" +
                "  'keyJavaClass' = 'java.lang.Long',\n" +
                "  'valueFormat' = 'java',\n" +
                "  'valueJavaClass' = 'com.hazelcast.simulator.tests.map.RegressionObject'" +
                "        )";

        try (SqlResult result = targetInstance.getSql().execute(query)) {
            System.out.println("Mapping created");
        }
    }

    @TimeStep(prob = 1)
    public void select() {
        String sql = " SELECT * FROM  \n" + name + " " +
                " WHERE doubleValue1 = 0.1 \n" +
                " OR doubleValue1 = 0.2 \n" +
                " OR doubleValue1 = 0.3 \n";
        SqlService sqlService = targetInstance.getSql();
        try (SqlResult result = sqlService.execute(sql)) {
            result.iterator().forEachRemaining(this::sink);
        }
    }

    private void sink(SqlRow sqlRow) {
    }

}
