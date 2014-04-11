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
package com.hazelcast.stabilizer.tasks;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.stabilizer.Coach;
import com.hazelcast.stabilizer.Workout;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.Serializable;
import java.util.concurrent.Callable;

public class InitWorkout implements Callable, Serializable, HazelcastInstanceAware {
    private final static ILogger log = Logger.getLogger(InitWorkout.class);

    private transient HazelcastInstance hz;
    private final byte[] content;
    private final Workout workout;

    public InitWorkout(Workout workout, byte[] zippedContent) {
        this.content = zippedContent;
        this.workout = workout;
    }

    @Override
    public Object call() throws Exception {
        try {
            log.info("Init InitWorkout:"+workout.getId());
            Coach coach = (Coach) hz.getUserContext().get(Coach.KEY_COACH);
            coach.initWorkout(workout,content);
            return null;
        } catch (Exception e) {
            log.severe("Failed to init InitWorkout", e);
            throw e;
        }
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hz) {
        this.hz = hz;
    }
}
