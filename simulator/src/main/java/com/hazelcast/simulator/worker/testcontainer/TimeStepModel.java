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

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.AnnotatedMethodRetriever;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.worker.testcontainer.Probability.loadTimeStepProbabilityArray;
import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

public class TimeStepModel {

    private final Class testClass;
    private final Class threadStateClass;
    private final List<Method> beforeRunMethods;
    private final List<Method> afterRunMethods;
    private final List<Method> timeStepMethods;
    private final Map<Method, Probability> probabilities;
    private final byte[] timeStepProbabilityArray;
    private final PropertyBinding propertyBinding;
    private final Constructor threadStateConstructor;

    public TimeStepModel(Class testClass, PropertyBinding propertyBinding) {
        this.propertyBinding = propertyBinding;
        this.testClass = testClass;
        this.beforeRunMethods = loadBeforeRunMethods();
        this.afterRunMethods = loadAfterRunMethods();
        this.timeStepMethods = loadTimeStepMethods();
        this.threadStateClass = loadThreadStateClass();
        this.threadStateConstructor = loadThreadStateConstructor();
        this.probabilities = loadProbabilities();
        this.timeStepProbabilityArray = loadTimeStepProbabilityArray(probabilities, getActiveTimeStepMethods());
    }

    public final Class getTestClass() {
        return testClass;
    }

    public final Class getThreadStateClass() {
        return threadStateClass;
    }

    public final List<Method> getBeforeRunMethods() {
        return beforeRunMethods;
    }

    public final List<Method> getAfterRunMethods() {
        return afterRunMethods;
    }

    public final List<Method> getTimeStepMethods() {
        return timeStepMethods;
    }

    public final List<Method> getActiveTimeStepMethods() {
        List<Method> result = new ArrayList<Method>();
        for (Method method : timeStepMethods) {
            if (probabilities.get(method).isLargerThanZero()) {
                result.add(method);
            }
        }
        return result;
    }

    public final Constructor getThreadStateConstructor() {
        return threadStateConstructor;
    }

    // just for testing
    Probability getProbability(String methodName) {
        for (Method method : getTimeStepMethods()) {
            if (method.getName().equals(methodName)) {
                return probabilities.get(method);
            }
        }
        return null;
    }

    private List<Method> loadBeforeRunMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, BeforeRun.class)
                .findAll();

        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(BeforeRun.class.getSimpleName(), methods);
        return methods;
    }

    private List<Method> loadAfterRunMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, AfterRun.class)
                .findAll();

        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(AfterRun.class.getSimpleName(), methods);
        return methods;
    }

    private List<Method> loadTimeStepMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, TimeStep.class)
                .findAll();

        // there is a bound on the max number of timestep methods so they fit into a byte
        // we can easily increase the number to 256 in the future
        if (methods.size() > Byte.MAX_VALUE) {
            throw new IllegalTestException(
                    testClass.getName() + " has more than 127 TimeStep methods, found: " + methods.size());
        }

        validateUniqueMethodNames(methods);
        validateModifiers(methods);
        validateTimeStepArguments(methods);
        return methods;
    }

    private void validateTimeStepArguments(List<Method> methods) {
        for (Method method : methods) {
            if (method.getParameterTypes().length > 2) {
                throw new IllegalTestException("TimeStep method '" + method + "' can't have more than two arguments");
            }
        }
    }

    private void validateUniqueMethodNames(List<Method> methods) {
        Set<String> names = new HashSet<String>();

        for (Method method : methods) {
            String methodName = method.getName();
            if (!names.add(methodName)) {
                throw new IllegalTestException(
                        testClass.getName() + " has two or more TimeStep methods with name '" + methodName + "'");
            }
        }
    }

    private void validateModifiers(List<Method> methods) {
        for (Method method : methods) {
            if (!isPublic(method.getModifiers())) {
                throw new IllegalTestException("method '" + method + "' should be public");
            }

            if (isStatic(method.getModifiers())) {
                throw new IllegalTestException("method '" + method + "' should be static");
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
                    throw new IllegalTestException(owner + " method '" + method + "' can't have more than one argument");
            }
        }
    }

    private Map<Method, Probability> loadProbabilities() {
        Map<Method, Probability> probMap = new HashMap<Method, Probability>();

        Method defaultMethod = null;
        Probability totalProbability = new Probability(0);
        for (Method method : timeStepMethods) {
            Probability timeStepProbability = loadProbability(method);
            if (timeStepProbability.isMinusOne()) {
                if (defaultMethod != null) {
                    throw new IllegalTestException("TimeStep method '" + method + "' can't have probability -1."
                            + " Method '" + defaultMethod + "' already has probability -1 and only one such method is allowed");
                }
                defaultMethod = method;
            } else if (timeStepProbability.isLargerThanOne()) {
                throw new IllegalTestException("TimeStep method '" + method + "'"
                        + " can't have a probability larger than 1, found: " + timeStepProbability);
            } else if (timeStepProbability.isSmallerThanZero()) {
                throw new IllegalTestException("TimeStep method '" + method + "'"
                        + " can't have a probability smaller than 0, found: " + timeStepProbability);
            } else {
                totalProbability = totalProbability.add(timeStepProbability);
                if (totalProbability.isLargerThanOne()) {
                    throw new IllegalTestException("TimeStep method '" + method + "' with probability " + timeStepProbability
                            + " exceeds the total probability of 1");
                }
                probMap.put(method, timeStepProbability);
            }
        }

        if (defaultMethod != null) {
            Probability probability = new Probability(1).sub(totalProbability);
            probMap.put(defaultMethod, probability);
        } else if (totalProbability.isSmallerThanOne()) {
            throw new IllegalTestException("The total probability of TimeStep methods in test " + testClass.getName()
                    + " is smaller than 1, found: " + totalProbability);
        }

        return probMap;
    }

    private Probability loadProbability(Method method) {
        String propertyName = method.getName() + "Prob";
        String valueString = propertyBinding.loadProperty(propertyName);

        double value;
        if (valueString == null) {
            // nothing was specified. So lets use what is on the annotation
            value = method.getAnnotation(TimeStep.class).prob();
        } else {
            // the user has explicitly configured a probability
            try {
                value = Double.parseDouble(valueString);
            } catch (NumberFormatException e) {
                throw new IllegalTestException(testClass.getName() + "." + propertyName
                        + " value '" + valueString + "' is not a valid double value", e);
            }
        }

        return new Probability(value);
    }

    /**
     * Returns the probabilities of the {@link TimeStep} methods.
     *
     * @return the array of probabilities for each {@link TimeStep} method
     * or {@code null} if there is only a single {@link TimeStep} method.
     *
     * The value in the byte refers to the index of the method in the {@link #getActiveTimeStepMethods()}. If a method has 0.5
     * probability and index 15, then 50% of the values in the array will point to 15.
     */
    @SuppressFBWarnings("EI_EXPOSE_REP")
    public byte[] getTimeStepProbabilityArray() {
        return timeStepProbabilityArray;
    }


    @SuppressWarnings("unchecked")
    private Constructor loadThreadStateConstructor() {
        if (threadStateClass == null) {
            return null;
        }

        Constructor constructor = null;
        try {
            constructor = threadStateClass.getDeclaredConstructor();
        } catch (NoSuchMethodException ignore) {
            ignore(ignore);
        }

        try {
            constructor = threadStateClass.getDeclaredConstructor(testClass);
        } catch (NoSuchMethodException ignore) {
            ignore(ignore);
        }

        if (constructor == null) {
            throw new IllegalTestException("Found no valid constructor for '" + threadStateClass.getName() + "'."
                    + " The constructor should have no arguments or one argument of type '" + threadStateClass.getName() + "'");
        }

        try {
            constructor.setAccessible(true);
        } catch (Exception e) {
            throw new IllegalTestException(e.getMessage(), e);
        }

        return constructor;
    }

    private Class loadThreadStateClass() {
        Set<Class> classes = new HashSet<Class>();
        collectThreadStateClass(classes, beforeRunMethods);
        collectThreadStateClass(classes, afterRunMethods);
        collectThreadStateClass(classes, timeStepMethods);

        if (classes.size() == 0) {
            // no first argument is found
            return null;
        }

        if (classes.size() > 1) {
            throw new IllegalTestException("More than one type of thread state class found: " + classes);
        }

        return classes.iterator().next();
    }

    private static void collectThreadStateClass(Set<Class> classes, List<Method> methods) {
        for (Method method : methods) {
            for (Class<?> paramType : method.getParameterTypes()) {
                if (paramType.isAssignableFrom(Probe.class)) {
                    continue;
                }

                if (paramType.isPrimitive()) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                            + " Thread state can't be a primitive.", method, paramType));
                }
                if (paramType.isInterface()) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                            + " Thread state can't be an interface.", method, paramType));
                }
                if (isAbstract(paramType.getModifiers())) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                            + " Thread state can't be an abstract.", method, paramType));
                }
                if (!isPublic(paramType.getModifiers())) {
                    throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                            + " Thread state should be public.", method, paramType));
                }

                classes.add(paramType);
            }
        }
    }
}
