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
package com.hazelcast.simulator.worker;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.protocol.operation.ExecuteScriptOperation;
import com.hazelcast.simulator.utils.BashCommand;
import com.hazelcast.simulator.utils.JavascriptCommand;
import com.hazelcast.simulator.utils.ThreadSpawner;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.getUserDir;
import static java.lang.String.format;

public class ScriptExecutor {
    private static final Logger LOGGER = Logger.getLogger(ScriptExecutor.class);

    private final HazelcastInstance hazelcastInstance;

    public ScriptExecutor(HazelcastInstance hazelcastInstance) {
        this.hazelcastInstance = hazelcastInstance;
    }

    public void execute(final ExecuteScriptOperation operation) {
        String fullCommand = operation.getCommand();
        int indexColon = fullCommand.indexOf(":");
        String type = fullCommand.substring(0, indexColon);
        final String command = fullCommand.substring(indexColon + 1);
        ThreadSpawner spawner = new ThreadSpawner(type + "[" + operation.getCommand() + "]");

        if (type.equals("js")) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object result = new JavascriptCommand(command)
                                .addEnvironment("hazelcastInstance", hazelcastInstance)
                                .execute();
                        LOGGER.info(format("Javascript [%s] with [%s]", command, result));
                    } catch (Exception e) {
                        LOGGER.warn(format("Failed to process javascript command '%s'", command), e);
                    }
                }
            });
        } else if (type.equals("bash")) {
            spawner.spawn(new Runnable() {
                @Override
                public void run() {
                    Map<String, Object> environment = new HashMap<String, Object>();
                    File pidFile = new File(getUserDir(), "worker.pid");
                    if (pidFile.exists()) {
                        environment.put("PID", fileAsText(pidFile));
                    }
                    new BashCommand(command)
                            .setDirectory(getUserDir())
                            .addEnvironment(environment)
                            .execute();
                }
            });
        } else {
            throw new IllegalArgumentException("Unhandled script type: " + type);
        }
    }
}
