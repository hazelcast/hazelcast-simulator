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
package com.hazelcast.simulator.jedis;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static java.lang.String.format;

public class JedisDriver extends VendorDriver<JedisCluster> {

    private static final int DEFAULT_REDIS_PORT = 6378;

    private JedisCluster client;

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
                .set("nodes", fileAsText("nodes.txt"))
                .set("file:worker.sh", loadWorkerScript("javaclient"));
    }

    @Override
    public JedisCluster getVendorInstance() {
        return client;
    }

    @Override
    public void startVendorInstance() throws Exception {
        Set<HostAndPort> addresses = getAddresses();
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        poolConfig.setMaxTotal(100);
        if (get("REDIS_CLUSTER_PASSWORD") != null) {
            this.client = new JedisCluster(addresses, 30000, 30000, 3, get("REDIS_CLUSTER_PASSWORD"), poolConfig);
        } else {
            this.client = new JedisCluster(addresses, poolConfig);
        }
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    private Set<HostAndPort> getAddresses() {
        String[] nodes = get("nodes").split(",");
        Set<HostAndPort> addresses = new HashSet<HostAndPort>();
        for (String node : nodes) {
            String[] addressParts = node.split(":");
            if (addressParts.length == 0 || addressParts.length > 2) {
                throw new IllegalArgumentException("Invalid node address. Example: localhost:11211");
            }

            int port = DEFAULT_REDIS_PORT;
            if (addressParts.length == 2) {
                port = Integer.parseInt(addressParts[1]);
            }
            addresses.add(new HostAndPort(addressParts[0], port));
        }

        return addresses;
    }
}
