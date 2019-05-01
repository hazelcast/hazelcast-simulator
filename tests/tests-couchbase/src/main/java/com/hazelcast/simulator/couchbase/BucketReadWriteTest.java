/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.simulator.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.cluster.BucketSettings;
import com.couchbase.client.java.cluster.ClusterManager;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.util.Random;

import static com.hazelcast.simulator.utils.GeneratorUtils.generateAsciiStrings;

public class BucketReadWriteTest extends CouchbaseTest {
    public int keyDomain = 100000;
    public int valueCount = 10000;
    public int minValueLength = 10;
    public int maxValueLength = 10;

    private Bucket bucket;
    private String[] values;

    @Setup
    public void setup() {
//        ClusterManager clusterManager = cluster.clusterManager("Administrator", "password");
//        BucketSettings bucketSettings = new DefaultBucketSettings.Builder()
//                .type(BucketType.COUCHBASE)
//                .name(name)
//                .replicas(0)
//                .quota(120)
//                .build();
//
//        clusterManager.insertBucket(bucketSettings);

        values = generateAsciiStrings(valueCount, minValueLength, maxValueLength);


        ClusterManager clusterManager = cluster.clusterManager("Administrator", "password");
        for (BucketSettings b : clusterManager.getBuckets()) {
            logger.info(b);
        }
        bucket = cluster.openBucket(name);
    }

    @Prepare(global = true)
    public void prepare() {
        Random random = new Random();
        for (int k = 0; k < keyDomain; k++) {
            JsonObject content = JsonObject.create().put("x", values[random.nextInt(valueCount)]);
            bucket.upsert(JsonDocument.create("" + k, content));
        }
    }

    @TimeStep(prob = 0.1)
    public JsonDocument put(ThreadState state) {
        int key = state.randomInt(keyDomain);
        JsonObject content = JsonObject.create().put("x", values[state.randomInt(valueCount)]);
        return bucket.upsert(JsonDocument.create("" + key, content));
    }

    @TimeStep(prob = -1)
    public JsonDocument get(ThreadState state) {
        int key = state.randomInt(keyDomain);
        return bucket.get("" + key);
    }

    public class ThreadState extends BaseThreadState {

    }
}
