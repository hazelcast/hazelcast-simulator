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
package com.hazelcast.simulator.agent.workerprocess;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Parameters for a (single) Simulator Worker Process.
 */
public class WorkerParameters {

    private final Map<String, String> map = new HashMap<String, String>();

    public Map<String, String> asMap() {
        return map;
    }

    public String get(String key) {
        return map.get(key);
    }

    public int intGet(String key) {
        return Integer.parseInt(map.get(key));
    }

    public String getWorkerType() {
        return map.get("WORKER_TYPE");
    }

    public WorkerParameters set(String key, Object value) {
        map.put(key, "" + value);
        return this;
    }

    public WorkerParameters setAll(Map<String, String> items) {
        map.putAll(items);
        return this;
    }

    public Set<Map.Entry<String, String>> entrySet() {
        return map.entrySet();
    }

    @Override
    public String toString() {
        return "WorkerParameters" + map;
    }
}
