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
package com.hazelcast.simulator.ignite2;

import com.hazelcast.simulator.agent.workerprocess.WorkerParameters;
import com.hazelcast.simulator.coordinator.ConfigFileTemplate;
import com.hazelcast.simulator.coordinator.registry.AgentData;
import com.hazelcast.simulator.drivers.Driver;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;


import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;

public class Ignite2Driver extends Driver<Ignite> {

    private static final Logger LOGGER = LogManager.getLogger(Ignite2Driver.class);
    private Ignite ignite;

    @Override
    public WorkerParameters loadWorkerParameters(String workerType, int agentIndex) {
        WorkerParameters params = new WorkerParameters()
                .setAll(properties)
                .set("WORKER_TYPE", workerType)
                .set("file:log4j.xml", loadLog4jConfig());

        if ("member".equals(workerType)) {
            loadServerParameters(params);
        } else if ("litemember".equals(workerType)) {
            loadClientParameters(params);
        } else {
            throw new IllegalArgumentException(format("Unsupported workerType [%s]", workerType));
        }

        return params;
    }

    private void loadServerParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", get("MEMBER_ARGS", ""))
                .set("file:ignite.xml", loadServerOrNativeClientConfig(false))
                .set("file:worker.sh", loadWorkerScript("member"));
    }

    private void loadClientParameters(WorkerParameters params) {
        params.set("JVM_OPTIONS", get("CLIENT_ARGS", ""))
                .set("file:ignite.xml", loadServerOrNativeClientConfig(true))
                .set("file:worker.sh", loadWorkerScript("litemember"));
    }

    private String loadServerOrNativeClientConfig(boolean client) {
        String config = loadConfigFile("Ignite configuration", "ignite.xml");

        ConfigFileTemplate template = new ConfigFileTemplate(config)
                .withAgents(agents);

        StringBuilder addresses = new StringBuilder();
        for (AgentData agent : agents) {
            addresses.append("<value>").append(agent.getPrivateAddress()).append("</value>");
        }

        template.addReplacement("<!--ADDRESSES-->", addresses.toString());
        template.addReplacement("<!--CLIENT_MODE-->", client);
        return template.render();
    }

    @Override
    public Ignite getDriverInstance() {
        return ignite;
    }

    @Override
    public void startDriverInstance() {
        String workerType = get("WORKER_TYPE");
        LOGGER.info(format("%s Ignite instance starting", workerType));
        ignite = Ignition.start(new File(getUserDir(), "ignite.xml").getAbsolutePath());
        LOGGER.info(format("%s Ignite instance started", workerType));
    }

    @Override
    public void close() {
        if (ignite != null) {
            ignite.close();
        }
    }
}
