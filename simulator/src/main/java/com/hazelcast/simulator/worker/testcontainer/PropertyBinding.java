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
package com.hazelcast.simulator.worker.testcontainer;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.EmptyProbe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeBuilder;
import com.hazelcast.simulator.worker.metronome.MetronomeType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindAll;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static java.lang.String.format;

/**
 * Responsible for injecting:
 * <ol>
 * <li>values in public fields</li>
 * <li>TestContext in fields annotated with {@link InjectTestContext}</li>
 * <li>HazelcastInstance in fields annotated with @{@link InjectHazelcastInstance}</li>
 * <li>Metronome instance in fields annotated with {@link InjectMetronome}</li>
 * <li>Probe instance in fields annotated with {@link InjectProbe}</li>
 * </ol>
 *
 * The {@link PropertyBinding} also keeps track of all used properties. This makes it possible to detect if there are any unused
 * properties (so properties which are not bound). See {@link #ensureNoUnusedProperties()}.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class PropertyBinding {

    // properties
    public int intervalUs;
    public double ratePerSecond;
    public MetronomeType metronomeType = MetronomeBuilder.DEFAULT_METRONOME_TYPE;
    public boolean accountForCoordinatedOmission = true;

    // if we want to measure latency. Normally this is always true; but in its current setting, hdr can cause contention
    // and I want a switch that turns of hdr recording. Perhaps that with some tuning this isn't needed.
    public boolean measureLatency = true;

    private final Class<? extends Probe> probeClass;
    private final Class<? extends Metronome> metronomeClass;
    private TestContextImpl testContext;
    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();
    private final TestCase testCase;
    private final Set<String> unusedProperties = new HashSet<String>();
    private final MetronomeBuilder metronomeBuilder = new MetronomeBuilder();

    public PropertyBinding(TestCase testCase) {
        this.testCase = testCase;
        this.unusedProperties.addAll(testCase.getProperties().keySet());
        unusedProperties.remove("class");

        bind(this);

        this.metronomeClass = loadMetronomeClass();
        this.probeClass = loadProbeClass();
    }

    public PropertyBinding setTestContext(TestContextImpl testContext) {
        this.testContext = testContext;
        return this;
    }

    public void ensureNoUnusedProperties() {
        if (!unusedProperties.isEmpty()) {
            throw new BindException(format("Unused properties %s have not been found on '%s'"
                    , unusedProperties, testCase.getClassname()));
        }
    }

    public Map<String, Probe> getProbeMap() {
        return probeMap;
    }

    public String loadProperty(String propertyName) {
        checkNotNull(propertyName, "propertyName can't be null");

        String value = testCase.getProperty(propertyName);
        if (value != null) {
            unusedProperties.remove(propertyName);
        }
        return value;
    }

    public void bind(Object object) {
        checkNotNull(object, "object can't be null");

        Class classType = object.getClass();
        do {
            for (Field field : classType.getDeclaredFields()) {
                inject(object, field);
            }
            classType = classType.getSuperclass();
        } while (classType != null);

        Set<String> used = bindAll(object, testCase);
        unusedProperties.removeAll(used);

        if (object instanceof PropertyBindingAware) {
            ((PropertyBindingAware) object).bind(this);
        }
    }

    private void inject(Object object, Field field) {
        Class fieldType = field.getType();
        if (field.isAnnotationPresent(InjectTestContext.class)) {
            assertFieldType(fieldType, TestContext.class, InjectTestContext.class);
            setFieldValue(object, field, testContext);
        } else if (field.isAnnotationPresent(InjectHazelcastInstance.class)) {
            assertFieldType(fieldType, HazelcastInstance.class, InjectHazelcastInstance.class);
            setFieldValue(object, field, testContext.getTargetInstance());
        } else if (field.isAnnotationPresent(InjectProbe.class)) {
            assertFieldType(fieldType, Probe.class, InjectProbe.class);
            Probe probe = getOrCreateProbe(getProbeName(field), isPartOfTotalThroughput(field));
            setFieldValue(object, field, probe);
        } else if (field.isAnnotationPresent(InjectMetronome.class)) {
            assertFieldType(fieldType, Metronome.class, InjectMetronome.class);
            Metronome metronome = metronomeBuilder.build();
            setFieldValue(object, field, metronome);
        }
    }

    private static void assertFieldType(Class fieldType, Class expectedFieldType, Class<? extends Annotation> annotation) {
        if (!expectedFieldType.equals(fieldType)) {
            throw new IllegalTestException(format("Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), fieldType.getName(), expectedFieldType.getName()));
        }
    }

    public Class<? extends Metronome> getMetronomeClass() {
        return metronomeClass;
    }

    private Class<? extends Metronome> loadMetronomeClass() {
        metronomeBuilder
                .withIntervalMicros(intervalUs)
                .withAccountForCoordinatedOmission(accountForCoordinatedOmission)
                .withMetronomeType(metronomeType);

        if (ratePerSecond > 0) {
            metronomeBuilder.withRatePerSecond(ratePerSecond);
        }

        return metronomeBuilder.getMetronomeClass();
    }

    public Class<? extends Probe> getProbeClass() {
        return probeClass;
    }

    private Class<? extends Probe> loadProbeClass() {
        return measureLatency ? HdrProbe.class : null;
    }

    public Probe getOrCreateProbe(String probeName, boolean partOfTotalThroughput) {
        if (probeClass == null) {
            return EmptyProbe.INSTANCE;
        }

        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = new HdrProbe(partOfTotalThroughput);
            probeMap.put(probeName, probe);
        }
        return probe;
    }
}