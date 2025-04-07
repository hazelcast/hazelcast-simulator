/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.tests.map.aggregation;

import com.hazelcast.aggregation.Aggregators;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.aggregate.AllOfAggregationBuilder;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
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

import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

public class AggregationTest extends HazelcastTest {

    // properties
    // the number of map entries
    public int entryCount = 10_000_000;
    public int localParallelism = 1;

    private IMap<Integer, DoubleCompactPojo> map;
    private IMap<Integer, DoubleCompactPojo> resultMap;

    public String mapName = getClass().getSimpleName();

    @Setup
    public void setUp() {
        this.map = targetInstance.getMap(mapName);
        this.resultMap = targetInstance.getMap(mapName + "_result");
    }

    @Prepare(global = true)
    public void prepare() {
        Streamer<Integer, DoubleCompactPojo> streamer = StreamerFactory.getInstance(map, 100);

        if (map.size() != entryCount) {
            map.clear();
            for (int i = 0; i < entryCount; i++) {
                var value = new DoubleCompactPojo(ThreadLocalRandom.current().nextDouble(),
                        ThreadLocalRandom.current().nextDouble(),
                        ThreadLocalRandom.current().nextDouble(),
                        ThreadLocalRandom.current().nextDouble());
                streamer.pushEntry(i, value);
            }
            streamer.await();
        } else {
            logger.info("Map size already is " + map.size());
        }


        SqlService sqlService = targetInstance.getSql();
        String query = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + mapName + " "
                + "(__key integer, n1 double, n2 double, n3 double, n4 double)"
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.Integer',\n"
                + "                'valueFormat' = 'compact',\n"
                + "                'valueCompactTypeName' = 'doubleCompactPojo'\n"
//                + "                'valueFormat' = 'java',\n"
//                + "                'valueJavaClass' = 'com.hazelcast.simulator.hz.aggregation.DoubleCompactPojo'\n"
                + "        )";

        sqlService.executeUpdate(query);

        /*
        logger.info("Using map: {}, current size {}", mapName, map.size());
        if (map.size() != entryCount) {
            map.clear();
            String insertQuery = "INSERT INTO " + mapName + " select v, rand(), rand(), rand(), rand()"
                    + " FROM TABLE(generate_series(1, " + entryCount + "))";
            sqlService.executeUpdate(insertQuery);
            if (map.size() != entryCount) {
                throw new AssertionError("Expected " + entryCount + " but got " + map.size());
            }
        } else {
            logger.info("Map size already is " + map.size());
        }*/

        String query2 = "CREATE EXTERNAL MAPPING IF NOT EXISTS " + mapName + "_result "
                + "(__key integer, sum1 double, sum2 double, sum3 double, sum4 double)"
                + "        TYPE IMap\n"
                + "        OPTIONS (\n"
                + "                'keyFormat' = 'java',\n"
                + "                'keyJavaClass' = 'java.lang.Integer',\n"
                + "                'valueFormat' = 'compact',\n"
                + "                'valueCompactTypeName' = 'resultPojo'\n"
                + "        )";
        sqlService.executeUpdate(query2);
    }

    @TimeStep(prob = 0)
    public void sum() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "SELECT 0, sum(\"n1\"),sum(\"n2\"),sum(\"n3\"),sum(\"n4\") FROM " + mapName;
        try (SqlResult result = sqlService.execute(query)) {
            int rowCount = 0;
            for (SqlRow row : result) {
                logger.debug("Row: {}", row);
                rowCount++;
            }
            if (rowCount != 1) {
                throw new IllegalArgumentException("Invalid row count [expected=1 , actual=" + rowCount + "]");
            }
        }
    }

    @TimeStep(prob = 0)
    public void insert() throws Exception {
        SqlService sqlService = targetInstance.getSql();
        String query = "sink into " + mapName + "_result " + " select 0, sum(\"n1\"),sum(\"n2\"),sum(\"n3\"),sum(\"n4\") FROM " + mapName;
        sqlService.executeUpdate(query);
    }

    @TimeStep(prob = 0)
    public void jet() throws Exception {
        Pipeline simpleAggregation = getPipeline();
        targetInstance.getJet().newJob(simpleAggregation, new JobConfig().setStoreMetricsAfterJobCompletion(true)).join();
    }

    @TimeStep(prob = 0)
    public void jetLight() throws Exception {
        Pipeline simpleAggregation = getPipeline();
        targetInstance.getJet().newLightJob(simpleAggregation).join();
    }

    @TimeStep(prob = 0)
    public void predicate() throws Exception {
        var result = map.aggregate(new AllOfAggregator<Integer, DoubleCompactPojo>()
                .add(Aggregators.doubleSum("n1"))
                .add(Aggregators.doubleSum("n2"))
                .add(Aggregators.doubleSum("n3"))
                .add(Aggregators.doubleSum("n4")));

        resultMap.put(0, new DoubleCompactPojo((Double) result.get(0), (Double) result.get(1), (Double) result.get(2), (Double) result.get(3)));
    }

    @TimeStep(prob = 0)
    public void predicateCustom() throws Exception {
        // note that these are 2 client-side invocations, could be also sent to ExecutorService on member side
        var result = map.aggregate(new CustomAggregator<>());
        resultMap.put(0, new DoubleCompactPojo(result.n1, result.n2, result.n3, result.n4));
    }

    private Pipeline getPipeline() {
        Pipeline simpleAggregation = Pipeline.create();

        BatchStage<Map.Entry<Integer, DoubleCompactPojo>> aggregatedNumbers = simpleAggregation.readFrom(Sources.map(map)).setLocalParallelism(localParallelism);
        AllOfAggregationBuilder<Map.Entry<Integer, DoubleCompactPojo>> builder = AggregateOperations.allOfBuilder();
        Tag<Double> n1 = builder.add(AggregateOperations.summingDouble(item -> item.getValue().n1));
        Tag<Double> n2 = builder.add(AggregateOperations.summingDouble(item -> item.getValue().n2));
        Tag<Double> n3 = builder.add(AggregateOperations.summingDouble(item -> item.getValue().n3));
        Tag<Double> n4 = builder.add(AggregateOperations.summingDouble(item -> item.getValue().n4));
        AggregateOperation1<Map.Entry<Integer, DoubleCompactPojo>, ?, ItemsByTag> aggrOp = builder.build();

        BatchStage<ItemsByTag> aggregateSumTags = aggregatedNumbers.aggregate(aggrOp);

        aggregateSumTags.map(result -> Util.entry(0, new DoubleCompactPojo(result.get(n1), result.get(n2), result.get(n3), result.get(n4))))
                .writeTo(Sinks.map(mapName + "_result"));
        return simpleAggregation;
    }
}

