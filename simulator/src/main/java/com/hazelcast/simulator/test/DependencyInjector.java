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
import com.hazelcast.simulator.worker.tasks.IMultipleProbesWorker;
import com.hazelcast.simulator.worker.tasks.IWorker;
import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.getProbeName;
import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.isPartOfTotalThroughput;
import static com.hazelcast.simulator.utils.PropertyBindingSupport.bind;
import static com.hazelcast.simulator.utils.ReflectionUtils.setFieldValue;
import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static com.hazelcast.simulator.worker.tasks.IWorker.DEFAULT_WORKER_PROBE_NAME;
import static java.lang.String.format;
import static org.apache.commons.lang3.text.WordUtils.capitalizeFully;

public class DependencyInjector {

    private static final Logger LOGGER = Logger.getLogger(DependencyInjector.class);

    static final String METRONOME_INTERVAL_PROPERTY_NAME = "metronomeIntervalUs";
    static final String METRONOME_TYPE_PROPERTY_NAME = "metronomeType";

    public int metronomeIntervalUs;
    public MetronomeType metronomeType = SLEEPING;

    private final Class<? extends Metronome> metronomeClass;
    private final TestContext testContext;
    private final Map<String, Probe> probeMap = new ConcurrentHashMap<String, Probe>();
    private final TestCase testCase;
    private final Set<String> unusedProperties = new HashSet<String>();

    public DependencyInjector(TestContext testContext, TestCase testCase) {
        this.testCase = testCase;
        this.testContext = testContext;
        this.unusedProperties.addAll(testCase.getProperties().keySet());
        unusedProperties.remove("class");

        inject(this);

        this.metronomeClass = loadMetronomeClass();
    }

    public void ensureAllPropertiesUsed() {
        if (!unusedProperties.isEmpty()) {
            StringBuilder sb = new StringBuilder("The following properties have not been found:");
            for (String unusedProperty : unusedProperties) {
                sb.append(" ")
                        .append(testCase.getClassname())
                        .append(".")
                        .append(unusedProperty);
            }
            throw new BindException(sb.toString());
        }
    }

    public Class getMetronomeClass() {
        return metronomeClass;
    }

    public Map<String, Probe> getProbeMap() {
        return probeMap;
    }

    public void inject(Object object) {
        Class classType = object.getClass();
        do {
            for (Field field : classType.getDeclaredFields()) {
                inject(object, field);
            }
            classType = classType.getSuperclass();
        } while (classType != null);
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

    private Metronome newMetronome(Field field) {
        if (metronomeClass == null) {
            return new EmptyMetronome();
        }
        try {
            Constructor<? extends Metronome> constructor = metronomeClass.getConstructor(Long.TYPE);
            return constructor.newInstance(TimeUnit.MICROSECONDS.toNanos(metronomeIntervalUs));
        } catch (Exception e) {
            throw new IllegalTestException("Failed to bind on " + InjectMetronome.class.getSimpleName()
                    + " field '" + field + "'", e);
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

    private static void assertFieldType(Class fieldType, Class expectedFieldType, Class<? extends Annotation> annotation) {
        if (!expectedFieldType.equals(fieldType)) {
            throw new IllegalTestException(format("Found %s annotation on field of type %s, but %s is required!",
                    annotation.getName(), fieldType.getName(), expectedFieldType.getName()));
        }
    }

    private Probe getOrCreateProbe(String probeName, boolean partOfTotalThroughput) {
        Probe probe = probeMap.get(probeName);
        if (probe == null) {
//            probe = (runWithWorkerIsLightweightProbe
//                    ? new ThroughputProbe(partOfTotalThroughput)
//                    : new HdrProbe(partOfTotalThroughput));
            probe = new HdrProbe(partOfTotalThroughput);
            probeMap.put(probeName, probe);
        }
        return probe;
    }

    private Map<Enum, Probe> createOperationProbeMap(Class<? extends IWorker> workerClass, IWorker worker) {
        if (!IMultipleProbesWorker.class.isAssignableFrom(workerClass)) {
            return null;
        }

        // remove the default worker probe
        probeMap.remove(DEFAULT_WORKER_PROBE_NAME);

        Map<Enum, Probe> operationProbes = new HashMap<Enum, Probe>();
        for (Enum operation : ((IMultipleProbesWorker) worker).getOperations()) {
            String probeName = capitalizeFully(operation.name(), '_').replace("_", "") + "Probe";
            operationProbes.put(operation, getOrCreateProbe(probeName, true));
        }
        return operationProbes;
    }

}
