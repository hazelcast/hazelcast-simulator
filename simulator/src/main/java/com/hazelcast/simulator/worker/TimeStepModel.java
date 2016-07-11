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
package com.hazelcast.simulator.worker;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.PropertyBinding;
import com.hazelcast.simulator.test.IllegalTestException;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.AnnotationReflectionUtils.findAllMethods;
import static java.lang.Math.pow;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;

public class TimeStepModel {

    static final int PROBABILITY_PRECISION = 3;
    static final int PROBABILITY_LENGTH = (int) round(pow(10, PROBABILITY_PRECISION));
    static final double PROBABILITY_INTERVAL = 1.0 / PROBABILITY_LENGTH;

    private final Class testClass;
    private final Class threadContextClass;
    private final List<Method> beforeRunMethods;
    private final List<Method> afterRunMethods;
    private final List<Method> timeStepMethods;
    private final Map<Method, Double> probabilities;
    private final byte[] timeStepProbabilityArray;
    private final PropertyBinding propertyBinding;
    private final Constructor threadContextConstructor;

    public TimeStepModel(Class testClass, PropertyBinding propertyBinding) {
        this.propertyBinding = propertyBinding;
        this.testClass = testClass;
        this.beforeRunMethods = loadBeforeRunMethods();
        this.afterRunMethods = loadAfterRunMethods();
        this.timeStepMethods = loadTimeStepMethods();
        this.threadContextClass = loadThreadContextClass();
        this.threadContextConstructor = loadThreadContextConstructor();
        this.probabilities = loadProbabilities();
        this.timeStepProbabilityArray = loadTimeStepProbabilityArray();
    }

    private List<Method> loadBeforeRunMethods() {
        List<Method> methods = findAllMethods(testClass, BeforeRun.class);
        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(BeforeRun.class.getSimpleName(), methods);
        return methods;
    }

    private List<Method> loadAfterRunMethods() {
        List<Method> methods = findAllMethods(testClass, AfterRun.class);
        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(AfterRun.class.getSimpleName(), methods);
        return methods;
    }

    private List<Method> loadTimeStepMethods() {
        List<Method> methods = findAllMethods(testClass, TimeStep.class);

        // there is a bound on the max number of timestep methods so they fit into a byte
        // we can easily increase the number to 256 in the future.
        if (methods.size() > Byte.MAX_VALUE) {
            throw new IllegalTestException(testClass.getName() + " has more than 127 TimeStep methods, found:" + methods.size());
        }

        validateUniqueMethodNames(methods);
        validateModifiers(methods);
        validateTimeStepArguments(methods);
        return methods;
    }

    private void validateTimeStepArguments(List<Method> methods) {
        for (Method method : methods) {
            if (method.getParameterTypes().length > 2) {
                throw new IllegalTestException("TimeStep method '" + method + "' can't have more than 2 argument");
            }
        }
    }

    private void validateUniqueMethodNames(List<Method> methods) {
        Set<String> names = new HashSet<String>();

        for (Method method : methods) {
            String methodName = method.getName();
            if (!names.add(methodName)) {
                throw new IllegalTestException(
                        testClass.getName() + " has 2 or more timeStep methods with name '" + methodName + "'");
            }
        }
    }

    private void validateModifiers(List<Method> methods) {
        for (Method method : methods) {
            if (!isPublic(method.getModifiers())) {
                throw new IllegalTestException("method '" + method + "' should be public");
            }
        }
    }

    private void validateBeforeAndAfterRunArguments(String owner, List<Method> methods) {
        for (Method method : methods) {
            switch (method.getParameterTypes().length) {
                case 0:
                    break;
                case 1:
                    if (Probe.class.isAssignableFrom(method.getParameterTypes()[0])) {
                        throw new IllegalTestException(owner + " method '" + method + "' can't have a Probe argument");
                    }
                    break;
                default:
                    throw new IllegalTestException(owner + " method '" + method + "' can't have more than 1 argument");
            }
        }
    }

    private Map<Method, Double> loadProbabilities() {
        Map<Method, Double> probabilities = new HashMap<Method, Double>();

        double totalProbability = 0;
        for (Method method : timeStepMethods) {

            double probability;
            String propertyName = method.getName() + "Prob";
            String configuredValue = propertyBinding.loadProperty(propertyName);
            if (configuredValue == null) {
                // nothing was specified. So lets use what is on the annotation
                TimeStep timeStep = method.getAnnotation(TimeStep.class);
                probability = timeStep.prob();
            } else {
                // the user has explicitly configured a probability
                try {
                    probability = Double.parseDouble(configuredValue);
                } catch (NumberFormatException e) {
                    throw new IllegalTestException(testClass.getName() + "." + propertyName
                            + " value '" + configuredValue + "' is not a valid double value", e);
                }
            }

            if (probability > 1) {
                throw new IllegalTestException("TimeStep method '" + method + "' "
                        + "can't have a probability larger than 1, found " + probability);
            }

            if (probability < 0) {
                throw new IllegalTestException("TimeStep method '" + method + "' "
                        + "can't have a probability smaller than 0, found " + probability);
            }

            totalProbability += probability;

            if (totalProbability > 1) {
                throw new IllegalTestException("TimeStep method '" + method + "' with probability "
                        + probability + " exceeds the total probability of 1");
            }
            probabilities.put(method, probability);
        }

        if (totalProbability < 1) {
            throw new IllegalTestException("The total probability of timeStep methods in test " + testClass.getName()
                    + " is smaller than 1. Found " + totalProbability);
        }

        return probabilities;
    }

    /**
     * Returns null if there is one or no timestep methods.
     *
     * @return
     */
    public byte[] getTimeStepProbabilityArray() {
        return timeStepProbabilityArray;
    }

    private byte[] loadTimeStepProbabilityArray() {
        if (getActiveTimeStepMethods().size() < 2) {
            return null;
        }

        byte[] result = new byte[PROBABILITY_LENGTH];
        int index = 0;

        List<Method> activeTimeStepMethods = getActiveTimeStepMethods();
        for (int k = 0; k < activeTimeStepMethods.size(); k++) {
            Method method = activeTimeStepMethods.get(k);
            double probability = probabilities.get(method);
            for (int i = 0; i < round(probability * result.length); i++) {
                if (index < result.length) {
                    result[index] = (byte) k;
                    index++;
                }
            }
        }

        return result;
    }

    public Class getTestClass() {
        return testClass;
    }

    public Class getThreadContextClass() {
        return threadContextClass;
    }

    public List<Method> getBeforeRunMethods() {
        return beforeRunMethods;
    }

    public List<Method> getAfterRunMethods() {
        return afterRunMethods;
    }

    public List<Method> getTimeStepMethods() {
        return timeStepMethods;
    }

    public List<Method> getActiveTimeStepMethods() {
        List<Method> result = new ArrayList<Method>();
        for (Method method : timeStepMethods) {
            double probability = probabilities.get(method);
            if (probability > 0) {
                result.add(method);
            }
        }
        return result;
    }

    public Constructor getThreadContextConstructor() {
        return threadContextConstructor;
    }

    private Constructor loadThreadContextConstructor() {
        if (threadContextClass == null) {
            return null;
        }

        Constructor constructor = null;
        try {
            constructor = threadContextClass.getDeclaredConstructor();
        } catch (NoSuchMethodException ignore) {
        }

        try {
            constructor = threadContextClass.getDeclaredConstructor(testClass);
        } catch (NoSuchMethodException ignore) {
        }

        if (constructor == null) {
            throw new IllegalTestException("No valid constructor found for " + constructor.getName() + ". "
                    + "The constructor should have no arguments or one argument of type '" + threadContextClass.getName() + "'");
        }

        try {
            constructor.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalTestException(e.getMessage(), e);
        }

        return constructor;
    }

    private Class loadThreadContextClass() {
        Set<Class> classes = new HashSet<Class>();
        collectThreadContextClass(classes, beforeRunMethods);
        collectThreadContextClass(classes, afterRunMethods);
        collectThreadContextClass(classes, timeStepMethods);

        if (classes.size() == 0) {
            // no first argument is found.
            return null;
        }

        if (classes.size() > 1) {
            throw new IllegalTestException("More than 1 type of Thread Context class found: " + classes);
        }

        return classes.iterator().next();
    }

    private static void collectThreadContextClass(Set<Class> classes, List<Method> methods) {
        for (Method method : methods) {
            for (Class paramType : method.getParameterTypes()) {
                if (paramType.isAssignableFrom(Probe.class)) {
                    continue;
                }

                if (paramType.isPrimitive()) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread context of type '%s'. "
                            + "Thread context can't be a primitive.", method, paramType));
                }

                if (paramType.isInterface()) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread context of type '%s'. "
                            + "Thread context can't be an interface.", method, paramType));
                }

                if (isAbstract(paramType.getModifiers())) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread context of type '%s'. "
                            + "Thread context can't be an abstract.", method, paramType));
                }

                if (!isPublic(paramType.getModifiers())) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread context of type '%s'. "
                            + "Thread context should be public", method, paramType));
                }

                classes.add(paramType);
            }
        }
    }
}
