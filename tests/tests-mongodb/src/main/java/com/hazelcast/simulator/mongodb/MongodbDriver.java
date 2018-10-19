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

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.vendors.VendorDriver;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import java.io.IOException;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;

public class MongodbDriver extends VendorDriver<MongoClient> {

    private MongoClient client;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        WorkerParameters params = new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("javaclient".equals(workerType)) {
            loadClientParameters(params);
        } else {
            throw new IllegalArgumentException(format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadClientParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", get("CLIENT_ARGS", ""))
                // currently just one node
                .set("node", fileAsText("node.txt"))
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    @Override
    public MongoClient getVendorInstance() {
        return client;
    }

    @Override
    public void startVendorInstance() throws Exception {
        String address = get("node");
        String[] addressParts = address.split(":");
        if (addressParts.length == 0 || addressParts.length > 2) {
            throw new IllegalArgumentException("Invalid node address. Example: localhost:11211");
        }
        StringBuilder sb = new StringBuilder("mongodb://").append(address);
        if (addressParts.length == 1) {
            sb.append(":27017"); //default MongoDB port
        }
        this.client = MongoClients.create(sb.toString());
    }


    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }
}
