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
package com.hazelcast.simulator.coordinator;

import com.hazelcast.simulator.test.TestPhase;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static java.lang.String.format;

public class TestPhaseListenerContainer {

    private static final Logger LOGGER = Logger.getLogger(TestPhaseListenerContainer.class);

    private final ConcurrentMap<Integer, TestPhaseListener> listenerMap = new ConcurrentHashMap<Integer, TestPhaseListener>();

    public Collection<TestPhaseListener> getListeners() {
        return listenerMap.values();
    }

    public void addListener(int testIndex, TestPhaseListener listener) {
        listenerMap.put(testIndex, listener);
    }

    public void updatePhaseCompletion(int testIndex, TestPhase testPhase) {
        TestPhaseListener listener = listenerMap.get(testIndex);
        if (listener == null) {
            LOGGER.error(format("Could not find listener for testIndex %d (%d listeners in total)", testIndex,
                    listenerMap.size()));
            return;
        }
        listener.completed(testPhase);
    }
}
