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
package com.hazelcast.simulator.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.probes.impl.HdrProbe;
import com.hazelcast.simulator.test.annotations.InjectHazelcastInstance;
import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.worker.metronome.BusySpinningMetronome;
import com.hazelcast.simulator.worker.metronome.EmptyMetronome;
import com.hazelcast.simulator.worker.metronome.Metronome;
import com.hazelcast.simulator.worker.metronome.MetronomeType;
import com.hazelcast.simulator.worker.metronome.SleepingMetronome;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;


/**
 * Responsible for injecting:
 * <ol>
 * <li>values in public fields</li>
 * <li>TestContext in fields annotated with {@link InjectTestContext}</li>
 * <li>HazelcastInstance in fields annotated with @{@link InjectHazelcastInstance}</li>
 * <li>Metronome instance in fields annotated with {@link InjectMetronome}</li>
 * <li>Probe instance in fields annotated with {@link InjectProbe}</li>
 * </ol>
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class PropertyBinding {

    // properties
    public int metronomeIntervalUs;
    public MetronomeType metronomeType = SLEEPING;
    public boolean measureLatency = true;

    private final Class<? extends Probe> probeClass;
    private final Class<? extends Metronome> metronomeClass;
    private TestContext testContext;
    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();
    private final TestCase testCase;
    private final Set<String> unusedProperties = new HashSet<String>();

    public PropertyBinding(TestCase testCase) {
        this.testCase = testCase;
        this.unusedProperties.addAll(testCase.getProperties().keySet());
        unusedProperties.remove("class");

        inject(this);

        this.metronomeClass = loadMetronomeClass();
        this.probeClass = loadProbeClass();
    }

    public PropertyBinding setTestContext(TestContext testContext) {
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
        String value = testCase.getProperty(propertyName);
        if (value != null) {
            unusedProperties.remove(propertyName);
        }
        return value;
    }

    public void inject(Object object) {
        checkNotNull(object, "object can't be null");

        Class classType = object.getClass();
        do {
            for (Field field : classType.getDeclaredFields()) {
                inject(object, field);
            }
            classType = classType.getSuperclass();
        } while (classType != null);

        if (object instanceof PropertyBindingAware) {
            ((PropertyBindingAware) object).inject(this);
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
            Metronome metronome = newMetronome(field);
            setFieldValue(object, field, metronome);
        } else {
            String propertyName = field.getName();
            if (bind(object, testCase, propertyName)) {
                unusedProperties.remove(propertyName);
            }
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

    private Metronome newMetronome(Field field) {
        if (metronomeClass == null) {
            return new EmptyMetronome();
        }

        try {
            Constructor<? extends Metronome> constructor = metronomeClass.getConstructor(Long.TYPE);
            return constructor.newInstance(MICROSECONDS.toNanos(metronomeIntervalUs));
        } catch (Exception e) {
            throw new IllegalTestException("Failed to bind " + InjectMetronome.class.getSimpleName()
                    + " on field '" + field + "'", e);
        }
    }

    private Class<? extends Metronome> loadMetronomeClass() {
        if (metronomeIntervalUs == 0) {
            return null;
        }

        switch (metronomeType) {
            case NOP:
                return null;
            case BUSY_SPINNING:
                return BusySpinningMetronome.class;
            case SLEEPING:
                return SleepingMetronome.class;
            default:
                throw new IllegalStateException("Unrecognized metronomeType:" + metronomeType);
        }
    }

    public Class<? extends Probe> getProbeClass() {
        return probeClass;
    }

    private Class<? extends Probe> loadProbeClass() {
        return measureLatency ? HdrProbe.class : null;
    }

    public Probe getOrCreateProbe(String probeName, boolean partOfTotalThroughput) {
        if (probeClass == null) {
            return null;
        }

        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = new HdrProbe(partOfTotalThroughput);
            probeMap.put(probeName, probe);
        }
        return probe;
    }
}
