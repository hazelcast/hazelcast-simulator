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
package com.hazelcast.simulator.mongodb;

import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

public class ReadWriteTest extends MongodbTest {

    public int itemCount = 100000;
    public int valueSize = 7;
    // Mongodb requires unique '_id' value. When any entry is updated then '_id' value has to be the same as original.
    // We provide 2D array which includes more values with same '_id' to allow entry update without creation new Object inside
    // put method. This field determines how many unique values is generated for every '_id'.
    public int idArraySize = 3;
    public String databaseName = "test";
    public String collectionName = "readWriteTest";

    private MongoCollection<Document> col;
    private Document[][] values;

    @Setup
    public void setUp() {
        if (itemCount <= 0) {
            throw new IllegalStateException("size must be larger than 0");
        }

        MongoDatabase database = client.getDatabase(databaseName);
        col = database.getCollection(collectionName);

        values = new Document[idArraySize][itemCount];
        for (int i = 0; i < idArraySize; i++) {
            for (int j = 0; j < itemCount; j++) {
                values[i][j] = createEntry(j);
            }
        }
    }

    @Prepare(global = true)
    public void prepare() {
        for (int i = 0; i < itemCount; i++) {
            col.insertOne(values[0][i]);
        }
    }

    @TimeStep(prob = 0.1)
    public void put(ThreadState state) {
        int id = state.randomId();
        col.replaceOne(Filters.eq("_id", id), state.randomValue(id));
    }

    @TimeStep(prob = -1)
    public void get(ThreadState state) {
        col.find(Filters.eq("_id", state.randomId())).first();
    }

    public class ThreadState extends BaseThreadState {

        private int randomId() {
            return randomInt(itemCount);
        }

        private Document randomValue(int id) {
            return values[randomInt(idArraySize)][id];
        }
    }

    @Teardown
    public void tearDown() {
        col.drop();
    }

    private Document createEntry(int id) {
        return new Document("_id", id)
                .append("stringVal", randomAlphanumeric(valueSize));
    }
}
