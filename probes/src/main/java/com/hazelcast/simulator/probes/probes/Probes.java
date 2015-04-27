/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.probes.probes;

import com.hazelcast.simulator.probes.probes.impl.ConcurrentProbe;
import com.hazelcast.simulator.probes.probes.impl.DisabledProbe;

import static java.lang.String.format;

/**
 * Factory class for probes.
 */
public final class Probes {

    private Probes() {
    }

    /**
     * Creates a probe instance.
     *
     * If the configuration for the given name is <tt>null</tt> a {@link DisabledProbe} instance will be returned.
     *
     * @param probeName              name of the probe
     * @param targetClassType        target class type the probe will be assigned to
     * @param probesConfiguration    configuration of the probe, for allowed types see {@see ProbesType}
     * @param <T>                    type of the probe which extends {@link SimpleProbe}
     * @return instance of a probe
     */
    public static <T extends SimpleProbe> T createProbe(String probeName, Class<T> targetClassType,
                                                        ProbesConfiguration probesConfiguration) {
        return internalCreateProbe(probeName, targetClassType, probesConfiguration, false);
    }

    /**
     * Creates a thread safe probe instance.
     *
     * If the configuration for the given name is <tt>null</tt> a {@link DisabledProbe} instance will be returned.
     *
     * @param probeName              name of the probe
     * @param targetClassType        target class type the probe will be assigned to
     * @param probesConfiguration    configuration of the probe, for allowed types see {@see ProbesType}
     * @param <T>                    type of the probe which extends {@link SimpleProbe}
     * @return thread safe instance of a probe
     */
    public static <T extends SimpleProbe> T createConcurrentProbe(String probeName, Class<T> targetClassType,
                                                                  ProbesConfiguration probesConfiguration) {
        return internalCreateProbe(probeName, targetClassType, probesConfiguration, true);
    }

    @SuppressWarnings("unchecked")
    private static <T extends SimpleProbe> T internalCreateProbe(String probeName, Class<T> targetClassType,
                                                                             ProbesConfiguration probesConfiguration,
                                                                             boolean createConcurrentProbe) {
        String config = probesConfiguration.getConfig(probeName);
        ProbesType probesType = (config == null) ? ProbesType.DISABLED : ProbesType.getProbeType(config);

        if (probesType == null) {
            throw new IllegalArgumentException(format("Probe \"%s\" has unknown configuration %s", probeName, config));
        }

        // check if targetClassType is allowed
        if (!probesType.isAssignableFrom(targetClassType)) {
            throw new ClassCastException(format("Probe \"%s\" of type %s does not match requested probe type %s",
                    probeName, probesType, targetClassType.getSimpleName()));
        }

        try {
            // return single instance for DISABLED probe
            if (probesType == ProbesType.DISABLED) {
                return (T) DisabledProbe.INSTANCE;
            }

            // return concurrent probe
            if (createConcurrentProbe) {
                return (T) new ConcurrentProbe(probesType);
            }

            // return new instance
            return (T) probesType.createInstance();
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }
}
