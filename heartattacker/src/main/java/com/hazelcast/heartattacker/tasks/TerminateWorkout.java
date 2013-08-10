/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.heartattacker.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.heartattacker.Coach;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;
import java.util.logging.Level;

import static java.lang.String.format;

public class TerminateWorkout implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(TerminateWorkout.class);

    private transient HazelcastInstance hz;

    @Override
    public Object call() throws Exception {
        log.info("TerminateWorkout");

        long startMs = System.currentTimeMillis();

        Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
        coach.terminateWorkout();

        long durationMs = System.currentTimeMillis() - startMs;
        log.info(format("TerminateWorkout finished in %s ms", durationMs));
        return null;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}
