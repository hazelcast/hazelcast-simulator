/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.worker.metronome;

/**
 * Used to clock a running task or Worker with a defined interval.
 *
 * Metronome is not threadsafe.
 */
public interface Metronome {

    /**
     * Waits next execution.
     *
     * This call returns the time the call was expected to executed. This can be used to prevent the co-ordinated omission
     * problem. For more information see: https://vanilla-java.github.io/2016/07/20/Latency-for-a-set-Throughput.html
     *
     * @return the time the call was supposed to execute.
     */
    long waitForNext();
}

