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
package com.hazelcast.simulator.utils;

import org.apache.log4j.Logger;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.util.HashMap;
import java.util.Map;

public class JavascriptCommand {
    private static final Logger LOGGER = Logger.getLogger(JavascriptCommand.class);

    private final String command;
    private Map<String, Object> environment = new HashMap<String, Object>();

    public JavascriptCommand(String command) {
        this.command = command;
    }

    public JavascriptCommand addEnvironment(String variable, Object value) {
        this.environment.put(variable, value);
        return this;
    }

    public Object execute() throws Exception {
        ScriptEngineManager engineManager = new ScriptEngineManager();
        ScriptEngine engine = engineManager.getEngineByExtension("js");
        for (Map.Entry<String, Object> entry : environment.entrySet()) {
            engine.put(entry.getKey(), entry.getValue());
        }

        LOGGER.info(command);
        return engine.eval(command);
    }
}
