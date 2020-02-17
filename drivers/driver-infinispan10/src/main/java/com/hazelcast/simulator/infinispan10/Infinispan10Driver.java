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
package com.hazelcast.simulator.infinispan10;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.vendors.VendorDriver;
import org.apache.log4j.Logger;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfiguration;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;

import java.util.Properties;

import static java.lang.String.format;

/**
 * todo
 * - logging in case of jgroups error
 * - fix the 'ping' port
 * - hotrod configuration
 *
 * done
 * - java assist for netty
 * - client connection terminates
 * - client support
 * - cluster support
 * - some sensible configuration for the cache
 * - right ports
 */
public class Infinispan10Driver extends VendorDriver<BasicCacheContainer> {

    private static final Logger LOGGER = Logger.getLogger(Infinispan10Driver.class);

    private BasicCacheContainer cacheContainer;
    private HotRodServer hotRodServer;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        WorkerParameters params = new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("member".equals(workerType)) {
            loadServerParameters(params, agents.get(agentIndex - 1));
        } else if ("javaclient".equals(workerType)) {
            loadClientParameters(params);
        } else {
            throw new IllegalArgumentException(format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadServerParameters(WorkerParameters params, AgentData agent) {
        String memberArgs = get("MEMBER_ARGS", "")
                + " -Djava.net.preferIPv4Stack=true"
                + " -Djgroups.bind_address=" + agent.getPrivateAddress()
                + " -Djgroups.tcp.address=" + agent.getPrivateAddress()
                + " -Djgroups.tcp.port=" + get("HAZELCAST_PORT")
                + " -Djgroups.tcpping.initial_hosts=" + initialHosts(false);

        params.set("JVM_OPTIONS", memberArgs)
                .set("file:infinispan.xml", loadConfigFile("Infinispan configuration", "infinispan.xml"))
                .set("file:worker.sh", loadWorkerScript("member"));
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
    public BasicCacheContainer getVendorInstance() {
        return cacheContainer;
    }

    @Override
    public void startVendorInstance() throws Exception {
        String workerType = get("WORKER_TYPE");
        if ("javaclient".equals(workerType)) {
             Properties hotrodProperties = new Properties();
            hotrodProperties.setProperty("infinispan.client.hotrod.server_list", get("server_list"));
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder().withProperties(hotrodProperties);
            Configuration configuration = configurationBuilder.build();
            RemoteCacheManager remoteCacheManager = new RemoteCacheManager(configuration);
            this.cacheContainer = remoteCacheManager;
        } else {
            DefaultCacheManager defaultCacheManager = new DefaultCacheManager("infinispan.xml");
            this.cacheContainer = defaultCacheManager;
            HotRodServerConfiguration hotRodServerConfiguration = new HotRodServerConfigurationBuilder()
                    .host(get("PRIVATE_ADDRESS")).port(11222).build();
            this.hotRodServer = new HotRodServer();
            hotRodServer.start(hotRodServerConfiguration, defaultCacheManager);
        }
    }

    @Override
    public void close() {
        if (hotRodServer != null) {
            hotRodServer.stop();
        }

        if (cacheContainer != null) {
            cacheContainer.stop();
        }
    }
}
