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
package com.hazelcast.simulator.lettucecluster6;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.drivers.Driver;
import io.lettuce.core.ReadFrom;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.masterreplica.MasterReplica;
import io.lettuce.core.masterreplica.StatefulRedisMasterReplicaConnection;

import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class Lettucecluster6Driver extends Driver<RedisClusterClient> {

    private RedisClusterClient client;

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
                .set("file:worker.sh", loadWorkerScript("javaclient"))
                .set("server_list", initialHosts(true));
    }

    private String initialHosts(boolean clientMode) {
        String port = clientMode ? get("CLIENT_PORT", "11222") : get("HAZELCAST_PORT");

        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (AgentData agent : agents) {
            if (first) {
                first = false;
            } else if (clientMode) {
                sb.append(';');
            } else {
                sb.append(',');
            }

            if (clientMode) {
                sb.append(agent.getPrivateAddress()).append(":").append(port);
            } else {
                sb.append(agent.getPrivateAddress()).append("[").append(port).append("]");
            }
        }
        return sb.toString();
    }

    @Override
    public RedisClusterClient getDriverInstance() {
        return client;
    }

    @Override
    public void startDriverInstance() throws Exception {
        String workerType = get("WORKER_TYPE");
        if ("javaclient".equals(workerType)) {
            String[] uris = get("URI").split(",");

            List<RedisURI> nodes = new LinkedList<>();
            for (String uri : uris) {
                nodes.add(RedisURI.create(uri));
            }

            client = RedisClusterClient.create(nodes);
        }
    }

    @Override
    public void close() {
        if (client != null) {
            client.shutdown();
        }
    }
}
