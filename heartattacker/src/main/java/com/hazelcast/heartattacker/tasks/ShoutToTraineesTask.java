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

public class ShoutToTraineesTask implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(ShoutToTraineesTask.class);

    private final Callable task;
    private final String taskDescription;
    private transient HazelcastInstance hz;

    public ShoutToTraineesTask(Callable task, String taskDescription) {
        this.task = task;
        this.taskDescription = taskDescription;
    }

    @Override
    public Object call() throws Exception {
        try {
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.shoutToTrainees(task, taskDescription);
        } catch (Exception e) {
            log.log(Level.SEVERE, format("Failed to execute [%s]", taskDescription), e);
            throw e;
        }

        return null;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        this.hz = hazelcastInstance;
    }
}

