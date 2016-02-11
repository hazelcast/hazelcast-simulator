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
package com.hazelcast.simulator.worker.tasks;

import com.hazelcast.simulator.probes.Probe;

import java.util.Map;
import java.util.Set;

/**
 * Interface for {@link IWorker} implementations, which should get individual {@link Probe} instances per operation injected.
 */
public interface IMultipleProbesWorker extends IWorker {

    /**
     * Returns a set of all defined operations.
     *
     * Is used by the {@link com.hazelcast.simulator.test.TestContainer} and should not be called by the user.
     *
     * @return {@link Set<Enum>} of all defined operations.
     */
    Set<? extends Enum> getOperations();

    /**
     * Sets the map with individual probes for each operation.
     *
     * Is used by the {@link com.hazelcast.simulator.test.TestContainer} and should not be called by the user.
     *
     * @param probeMap {@link Map} with individual probes per operation.
     */
    void setProbeMap(Map<? extends Enum, Probe> probeMap);
}
