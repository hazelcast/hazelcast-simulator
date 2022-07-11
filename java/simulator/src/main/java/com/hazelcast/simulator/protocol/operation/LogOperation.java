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
package com.hazelcast.simulator.protocol.operation;


import org.apache.logging.log4j.Level;

/**
 * Writes the message with the requested log level to the local logging framework.
 */
public class LogOperation implements SimulatorOperation {

    /**
     * Defines the message which should be logged.
     */
    private final String message;

    /**
     * Defines the desired log level of the message.
     */
    private final String level;

    public LogOperation(String message) {
        this(message, Level.INFO);
    }

    public LogOperation(String message, Level level) {
        this.message = message;
        this.level = level.toString();
    }

    public String getMessage() {
        return message;
    }

    public Level getLevel() {
        return Level.toLevel(level, Level.INFO);
    }

    @Override
    public String toString() {
        return "LogOperation{"
                + "message='" + message + '\''
                + ", level='" + level + '\''
                + '}';
    }
}
