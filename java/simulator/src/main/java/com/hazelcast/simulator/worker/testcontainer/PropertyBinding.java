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

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.probes.impl.HdrLatencyProbe;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.InjectDriver;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.PropertyBindingSupport;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bindAll;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.removeUnderscores;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static java.lang.String.format;

/**
 * Responsible for injecting:
 * <ol>
 * <li>values in public fields</li>
 * <li>TestContext in fields annotated with {@link InjectTestContext}</li>
 * <li>HazelcastInstance in fields annotated with @{@link InjectDriver}</li>
 * </ol>
 * <p>
 * The {@link PropertyBinding} also keeps track of all used properties. This makes it possible to detect if there are any unused
 * properties (so properties which are not bound). See {@link #ensureNoUnusedProperties()}.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class PropertyBinding {

    static final int DEFAULT_THREAD_COUNT = 10;
    static final int DEFAULT_RECORD_JITTER_THRESHOLD_NS = 1000;

    // if we want to measure latency. Normally this is always true; but in its current setting, hdr can cause contention
    // and I want a switch that turns of hdr recording. Perhaps that with some tuning this isn't needed.
    public boolean measureLatency = true;
    // flag to enable jitter recording
    public boolean recordJitter;
    // configures the minimum value for the jitter sample to be recorded.
    public int recordJitterThresholdNs = DEFAULT_RECORD_JITTER_THRESHOLD_NS;

    // this can be removed as soon as the @InjectMetronome/worker functionality is dropped
    private MetronomeSupplier workerMetronomeConstructor;
    private final Class<? extends LatencyProbe> probeClass;
    private TestContextImpl testContext;
    private final TestCase testCase;
    private final Set<String> unusedProperties = new HashSet<>();
    private Object driverInstance;

    public PropertyBinding(TestCase testCase) {
        this.testCase = testCase;
        this.unusedProperties.addAll(testCase.getProperties().keySet());
        unusedProperties.remove("class");
        unusedProperties.remove("rampupSeconds");

        bind(this);

        if (recordJitterThresholdNs < 0) {
            throw new IllegalTestException("recordJitterThresholdNs can't be smaller than 0");
        }

        this.workerMetronomeConstructor = new MetronomeSupplier(
                "", this, loadAsInt("threadCount", DEFAULT_THREAD_COUNT));
        this.probeClass = loadProbeClass();
    }

    public PropertyBinding setDriverInstance(Object driverInstance) {
        this.driverInstance = driverInstance;
        return this;
    }

    public PropertyBinding setTestContext(TestContextImpl testContext) {
        this.testContext = testContext;
        return this;
    }

    public void ensureNoUnusedProperties() {
        if (!unusedProperties.isEmpty()) {
            throw new BindException(format("The following properties %s have not been used on '%s'"
                    , unusedProperties, testCase.getClassname()));
        }
    }

    public String load(String property) {
        checkNotNull(property, "propertyName can't be null");

        String value = testCase.getProperty(property);
        if (value != null) {
            unusedProperties.remove(property);
        }
        return value;
    }

    public int loadAsInt(String property, int defaultValue) {
        String value = load(property);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Integer.parseInt(removeUnderscores(value));
        } catch (NumberFormatException e) {
            throw new IllegalTestException(format("Property [%s] with value [%s] is not an int", property, value));
        }
    }

    public long loadAsLong(String property, long defaultValue) {
        String value = load(property);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Long.parseLong(removeUnderscores(value));
        } catch (NumberFormatException e) {
            throw new IllegalTestException(format("Property [%s] with value [%s] is not an long", property, value));
        }
    }

    public double loadAsDouble(String property, double defaultValue) {
        String value = load(property);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Double.parseDouble(removeUnderscores(value));
        } catch (NumberFormatException e) {
            throw new IllegalTestException(format("Property [%s] with value [%s] is not a double", property, value));
        }
    }

    public boolean loadAsBoolean(String property, boolean defaultValue) {
        String value = load(property);
        if (value == null) {
            return defaultValue;
        }

        try {
            return Boolean.parseBoolean(value);
        } catch (NumberFormatException e) {
            throw new IllegalTestException(format("Property [%s] with value [%s] is not a double", property, value));
        }
    }

    public <E> Class<E> loadAsClass(String property, Class<E> defaultValue) {
        String value = load(property);
        if (value == null) {
            return defaultValue;
        }

        try {
            return (Class<E>) PropertyBindingSupport.class.getClassLoader().loadClass(value);
        } catch (ClassNotFoundException e) {
            throw new IllegalTestException(
                    format("Property [%s] with value [%s] points to a non existing class", property, value));
        }
    }

    public static String toPropertyName(String prefix, String name) {
        return prefix.equals("") ? name : prefix + capitalizeFirst(name);
    }

    public static String capitalizeFirst(String s) {
        return s.substring(0, 1).toUpperCase() + s.substring(1);
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
    }

    private void inject(Object object, Field field) {
        Class fieldType = field.getType();
        if (field.isAnnotationPresent(InjectTestContext.class)) {
            assertFieldType(fieldType, TestContext.class, InjectTestContext.class);
            setFieldValue(object, field, testContext);
        } else if (field.isAnnotationPresent(InjectDriver.class)) {
            if (driverInstance == null) {
                throw new IllegalTestException("No driver found");
            }

            Class driverType = driverInstance.getClass();
            assertFieldType(driverType, fieldType, InjectDriver.class);
            setFieldValue(object, field, driverInstance);
        }
    }

    private static void assertFieldType(Class actualType, Class requiredType, Class<? extends Annotation> annotation) {
        if (!requiredType.isAssignableFrom(actualType)) {
            throw new IllegalTestException(format("Wrong type. "
                            + "Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), actualType.getName(), requiredType.getName()));
        }
    }

    public Class<? extends LatencyProbe> getProbeClass() {
        return probeClass;
    }

    private Class<? extends LatencyProbe> loadProbeClass() {
        return measureLatency ? HdrLatencyProbe.class : null;
    }

    public TestCase getTestCase() {
        return testCase;
    }

    public TestContextImpl getTestContext() {
        return testContext;
    }
}
