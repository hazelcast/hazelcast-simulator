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
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.test.annotations.InjectTestContext;
import com.hazelcast.simulator.test.annotations.InjectVendor;
import com.hazelcast.simulator.utils.BindException;
import com.hazelcast.simulator.utils.PropertyBindingSupport;

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
 * <li>HazelcastInstance in fields annotated with @{@link InjectVendor}</li>
 * <li>Probe instance in fields annotated with {@link InjectProbe}</li>
 * </ol>
 * <p>
 * The {@link PropertyBinding} also keeps track of all used properties. This makes it possible to detect if there are any unused
 * properties (so properties which are not bound). See {@link #ensureNoUnusedProperties()}.
 */
@SuppressWarnings("checkstyle:visibilitymodifier")
public class PropertyBinding {

    // if we want to measure latency. Normally this is always true; but in its current setting, hdr can cause contention
    // and I want a switch that turns of hdr recording. Perhaps that with some tuning this isn't needed.
    public boolean measureLatency = true;

    private final Class<? extends Probe> probeClass;
    private TestContextImpl testContext;
    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();
    private final TestCase testCase;
    private final Set<String> unusedProperties = new HashSet<String>();
    private Object vendorInstance;

    public PropertyBinding(TestCase testCase) {
        this.testCase = testCase;
        this.unusedProperties.addAll(testCase.getProperties().keySet());
        unusedProperties.remove("class");
        unusedProperties.remove("warmupMillis");

        bind(this);

        this.probeClass = loadProbeClass();
    }

    public PropertyBinding setVendorInstance(Object vendorInstance) {
        this.vendorInstance = vendorInstance;
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

    public Map<String, Probe> getProbeMap() {
        return probeMap;
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
            return Integer.parseInt(value);
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
            return Long.parseLong(value);
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
            return Double.parseDouble(value);
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
        if (prefix.equals("")) {
            return name;
        }

        return prefix + capitalizeFirst(name);
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
            setFieldValue(object, field, vendorInstance);
        } else if (field.isAnnotationPresent(InjectVendor.class)) {
            if (vendorInstance == null) {
                throw new IllegalTestException("No vendor found");
            }

            Class vendorType = vendorInstance.getClass();
            assertFieldType(vendorType, fieldType, InjectVendor.class);
            setFieldValue(object, field, vendorInstance);
        } else if (field.isAnnotationPresent(InjectProbe.class)) {
            assertFieldType(fieldType, Probe.class, InjectProbe.class);
            Probe probe = getOrCreateProbe(getProbeName(field), isPartOfTotalThroughput(field));
            setFieldValue(object, field, probe);
        }
    }

    private static void assertFieldType(Class actualType, Class requiredType, Class<? extends Annotation> annotation) {
        if (!requiredType.isAssignableFrom(actualType)) {
            throw new IllegalTestException(format("Wrong type. "
                            + "Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), actualType.getName(), requiredType.getName()));
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
            return EmptyProbe.INSTANCE;
        }

        Probe probe = probeMap.get(probeName);
        if (probe == null) {
            probe = new HdrProbe(partOfTotalThroughput);
            probeMap.put(probeName, probe);
        }
        return probe;
    }

    public TestContextImpl getTestContext() {
        return testContext;
    }
}
