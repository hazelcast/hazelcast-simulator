package com.hazelcast.simulator.worker.loadsupport;

/**
 * MapStreamer is used for map initialization during a warm-up phase.
 *
 * With Hazelcast version 3.5 or newer it does use asynchronous IMap operations so it's extremely fast,
 * but it has own back-pressure and doesn't rely on back-pressure provided by Hazelcast.
 *
 * For older Hazelcast versions a synchronous version is created by the factory.
 *
 * <pre>
 * {@code
 *   MapStreamer<String, Person> streamer = MapStreamerFactory.getInstance(map);
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
public interface MapStreamer<K, V> {

    /**
     * Push key/value pair into a map. It's a non-blocking operation.
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
