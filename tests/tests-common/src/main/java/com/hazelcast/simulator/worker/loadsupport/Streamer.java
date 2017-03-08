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
package com.hazelcast.simulator.worker.loadsupport;

/**
 * Streamer is used for storage initialization during a warm-up phase.
 *
 * With Hazelcast version 3.5 or newer it does use asynchronous operations so it's extremely fast,
 * but it has own back-pressure and doesn't rely on back-pressure provided by Hazelcast.
 *
 * For older Hazelcast versions a synchronous version is created by the factory.
 *
 * <pre>
 * {@code
 *   Streamer<String, Person> streamer = StreamerFactory.getInstance(map);
 *   for (int i = 0; i < keyCount; i++) {
 *     String key = generateString(keyLength);
 *     Person value = new Person(i);
 *     streamer.pushEntry(key, value);
 *   }
 *   streamer.await();
 * }
 * </pre>
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface Streamer<K, V> {

    /**
     * Default concurrency level for asynchronous {@link Streamer} instances.
     */
    int DEFAULT_CONCURRENCY_LEVEL = 1000;

    /**
     * Push key/value pair into a storage. It's a non-blocking operation.
     * You have to call {@link #await()} to make sure the entry has been created successfully.
     *
     * @param key   the key of the map entry
     * @param value the new value of the map entry
     */
    void pushEntry(K key, V value);

    /**
     * Wait until all in-flight operations are finished.
     *
     * @throws RuntimeException if at least any pushEntry operation failed
     */
    void await();
}
