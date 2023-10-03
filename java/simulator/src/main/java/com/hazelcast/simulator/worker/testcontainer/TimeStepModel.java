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

import com.hazelcast.simulator.probes.LatencyProbe;
import com.hazelcast.simulator.test.annotations.AfterRun;
import com.hazelcast.simulator.test.annotations.BeforeRun;
import com.hazelcast.simulator.test.annotations.StartNanos;
import com.hazelcast.simulator.test.annotations.TimeStep;
import com.hazelcast.simulator.utils.AnnotatedMethodRetriever;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.EmptyStatement.ignore;
import static com.hazelcast.simulator.worker.testcontainer.Probability.loadTimeStepProbabilityArray;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

public class TimeStepModel {

    private static final String PROB = "Prob";
    private final Class testClass;

    private final Map<String, ExecutionGroup> executionGroups = new HashMap<>();
    private final PropertyBinding propertyBinding;

    public TimeStepModel(Class testClass, PropertyBinding propertyBinding) {
        this.propertyBinding = propertyBinding;
        this.testClass = testClass;

        loadTimeStepMethods();
        loadBeforeRunMethods();
        loadAfterRunMethods();

        probabilitySanityCheck();

        for (ExecutionGroup executionGroup : executionGroups.values()) {
            executionGroup.init();
        }
    }

    // Checks if every probability has a matching timestep method. Otherwise you get confusing messages about total probability
    // not being 1.
    private void probabilitySanityCheck() {
        Set<String> unmatchedProbabilities = new HashSet<>();

        Set<String> timestepMethodNames = new HashSet<>();
        for (ExecutionGroup executionGroup : executionGroups.values()) {
            for (Method method : executionGroup.timeStepMethods) {
                timestepMethodNames.add(method.getName());
            }
        }

        for (String propertyName : propertyBinding.getTestCase().getProperties().keySet()) {
            if (!propertyName.endsWith(PROB)) {
                continue;
            }

            String methodName = propertyName.substring(0, propertyName.length() - PROB.length());
            if (!timestepMethodNames.contains(methodName)) {
                unmatchedProbabilities.add(propertyName);
            }
        }

        if (!unmatchedProbabilities.isEmpty()) {
            throw new IllegalTestException(
                    "The test contains probabilities without matching timestep method. "
                            + "Unmatched probabilities " + unmatchedProbabilities);
        }
    }

    public final Class getTestClass() {
        return testClass;
    }

    public final Class getThreadStateClass(String executionGroup) {
        return executionGroups.get(executionGroup).threadStateClass;
    }

    public final Set<String> getExecutionGroups() {
        return executionGroups.keySet();
    }

    public final List<Method> getBeforeRunMethods(String executionGroup) {
        return executionGroups.get(executionGroup).beforeRunMethods;
    }

    public final List<Method> getAfterRunMethods(String executionGroup) {
        return executionGroups.get(executionGroup).afterRunMethods;
    }

    public final List<Method> getActiveTimeStepMethods(String group) {
        List<Method> result = new ArrayList<>();
        ExecutionGroup executionGroup = executionGroups.get(group);
        for (Method method : executionGroup.timeStepMethods) {
            if (executionGroup.probabilities.get(method).isLargerThanZero()) {
                result.add(method);
            }
        }
        return result;
    }

    public final Constructor getThreadStateConstructor(String executionGroup) {
        return executionGroups.get(executionGroup).threadStateConstructor;
    }

    // just for testing
    Probability getProbability(String group, String methodName) {
        ExecutionGroup executionGroup = executionGroups.get(group);
        for (Method method : executionGroup.timeStepMethods) {
            if (method.getName().equals(methodName)) {
                return executionGroup.probabilities.get(method);
            }
        }
        return null;
    }

    private void loadBeforeRunMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, BeforeRun.class)
                .findAll();

        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(BeforeRun.class.getSimpleName(), methods);

        for (Method method : methods) {
            BeforeRun beforeRun = method.getAnnotation(BeforeRun.class);
            String executionGroupName = beforeRun.executionGroup();
            ensureExecutionGroupIsIdentifier(method, executionGroupName);
            ExecutionGroup executionGroup = executionGroups.get(executionGroupName);
            if (executionGroup == null) {
                if (executionGroupName.equals("")) {
                    throw new IllegalTestException(
                            "@BeforeRun " + method + " is part of default executionGroup,"
                                    + " but no timeStep methods for that executionGroup exist ");
                } else {
                    throw new IllegalTestException(
                            "@BeforeRun " + method + " is part of executionGroup [" + executionGroupName
                                    + "], but no timeStep methods for that executionGroup exist ");
                }
            }
            executionGroup.beforeRunMethods.add(method);
        }
    }

    private void loadAfterRunMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, AfterRun.class)
                .findAll();

        validateModifiers(methods);
        validateBeforeAndAfterRunArguments(AfterRun.class.getSimpleName(), methods);
        for (Method method : methods) {
            AfterRun afterRun = method.getAnnotation(AfterRun.class);
            String executionGroupName = afterRun.executionGroup();
            ensureExecutionGroupIsIdentifier(method, executionGroupName);
            ExecutionGroup executionGroup = executionGroups.get(executionGroupName);
            if (executionGroup == null) {
                if (executionGroupName.equals("")) {
                    throw new IllegalTestException(
                            "@AfterRun " + method + " is part of default executionGroup,"
                                    + " but no timeStep methods for that executionGroup exist ");
                } else {
                    throw new IllegalTestException(
                            "@AfterRun " + method + " is part of executionGroup [" + executionGroupName
                                    + "], but no timeStep methods for that executionGroup exist ");
                }
            }
            executionGroup.afterRunMethods.add(method);
        }
    }

    private void loadTimeStepMethods() {
        List<Method> methods = new AnnotatedMethodRetriever(testClass, TimeStep.class).findAll();

        // there is a bound on the max number of timestep methods so they fit into a byte
        // we can easily increase the number to 256 in the future
        if (methods.size() > Byte.MAX_VALUE) {
            throw new IllegalTestException(
                    testClass.getName() + " has more than 127 TimeStep methods, found: " + methods.size());
        }

        validateUniqueMethodNames(methods);
        validateModifiers(methods);
        validateTimeStepParameters(methods);

        for (Method method : methods) {
            TimeStep timeStep = method.getAnnotation(TimeStep.class);
            String group = timeStep.executionGroup();
            ensureExecutionGroupIsIdentifier(method, group);
            ExecutionGroup executionGroup = executionGroups.get(group);
            if (executionGroup == null) {
                executionGroup = new ExecutionGroup(group);
                executionGroups.put(group, executionGroup);
            }

            executionGroup.timeStepMethods.add(method);
        }
    }

    private static void ensureExecutionGroupIsIdentifier(Method method, String executionGroup) {
        if (executionGroup.equals("")) {
            return;
        }
        if (!isValidJavaIdentifier(executionGroup)) {
            throw new IllegalTestException(
                    method + " is using an invalid identifier for executionGroup [" + executionGroup + "]");
        }
    }

    private static boolean isValidJavaIdentifier(String s) {
        if (s.isEmpty()) {
            return false;
        }
        if (!Character.isJavaIdentifierStart(s.charAt(0))) {
            return false;
        }
        for (int i = 1; i < s.length(); i++) {
            if (!Character.isJavaIdentifierPart(s.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private void validateTimeStepParameters(List<Method> methods) {
        for (Method method : methods) {
            int parameterCount = method.getParameterTypes().length;
            if (parameterCount > 3) {
                throw new IllegalTestException("TimeStep method '" + method + "' can't have more than two arguments");
            }

            Class<?>[] parameterTypes = method.getParameterTypes();
            for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++) {
                if (!hasStartNanosAnnotation(method, parameterIndex)) {
                    continue;
                }

                Class parameterType = parameterTypes[parameterIndex];
                if (!Long.TYPE.equals(parameterType)) {
                    throw new IllegalTestException("TimeStep method '" + method + "' contains an illegal "
                            + StartNanos.class.getSimpleName() + " parameter at index " + parameterIndex
                            + ". Only type: long is allowed but found: " + parameterType.getName());
                }
            }
        }
    }

    public boolean hasStartNanosAnnotation(Method method, int parameterIndex) {
        Annotation[][] parametersAnnotations = method.getParameterAnnotations();
        Annotation[] parameterAnnotations = parametersAnnotations[parameterIndex];
        for (Annotation annotation : parameterAnnotations) {
            if ((annotation instanceof StartNanos)) {
                return true;
            }
        }
        return false;
    }

    private void validateUniqueMethodNames(List<Method> methods) {
        Set<String> names = new HashSet<>();
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
                    if (LatencyProbe.class.isAssignableFrom(method.getParameterTypes()[0])) {
                        throw new IllegalTestException(owner + " method '" + method + "' can't have a Probe argument");
                    }
                    break;
                default:
                    throw new IllegalTestException(owner + " method '" + method + "' can't have more than one argument");
            }
        }
    }

    /**
     * Returns the probabilities of the {@link TimeStep} methods.
     *
     * The value in the byte refers to the index of the method in the {@link #getActiveTimeStepMethods(String)}.
     * If a method has 0.5 probability and index 15, then 50% of the values in the array will point to 15.
     *
     * @param group the name of the execution group to get the probability array for
     * @return the array of probabilities for each {@link TimeStep} method or {@code null} if there is only a
     * single {@link TimeStep} method.
     */
    public byte[] getTimeStepProbabilityArray(String group) {
        return executionGroups.get(group).timeStepProbabilityArray;
    }

    private final class ExecutionGroup {
        private final List<Method> beforeRunMethods = new LinkedList<>();
        private final List<Method> afterRunMethods = new LinkedList<>();
        private final List<Method> timeStepMethods = new LinkedList<>();
        private final String name;
        private Class threadStateClass;
        private Constructor threadStateConstructor;
        private Map<Method, Probability> probabilities;
        private byte[] timeStepProbabilityArray;

        private ExecutionGroup(String name) {
            this.name = name;
        }

        private void init() {
            threadStateClass = loadThreadStateClass();
            threadStateConstructor = loadThreadStateConstructor();
            probabilities = loadProbabilities();
            timeStepProbabilityArray = loadTimeStepProbabilityArray(probabilities, getActiveTimeStepMethods(name));
        }

        private Class loadThreadStateClass() {
            Set<Class> classes = new HashSet<>();
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

        private void collectThreadStateClass(Set<Class> classes, List<Method> methods) {
            for (Method method : methods) {
                Class<?>[] parameterTypes = method.getParameterTypes();
                for (int parameterIndex = 0; parameterIndex < parameterTypes.length; parameterIndex++) {
                    Class<?> paramType = parameterTypes[parameterIndex];

                    if (paramType.isAssignableFrom(LatencyProbe.class)
                            || hasStartNanosAnnotation(method, parameterIndex)) {
                        continue;
                    }

                    if (paramType.isPrimitive()) {
                        throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                                + " Thread state can't be a primitive.", method, paramType));
                    } else if (paramType.isInterface()) {
                        throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                                + " Thread state can't be an interface.", method, paramType));
                    } else if (isAbstract(paramType.getModifiers())) {
                        throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                                + " Thread state can't be abstract.", method, paramType));
                    } else if (!isPublic(paramType.getModifiers())) {
                        throw new IllegalTestException(format("Method '%s' contains an illegal thread state of type '%s'."
                                + " Thread state should be public.", method, paramType));
                    }
                    classes.add(paramType);
                }
            }
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
                        + " The constructor should have no arguments or one argument "
                        + "of type '" + threadStateClass.getName() + "'");
            }

            try {
                constructor.setAccessible(true);
            } catch (Exception e) {
                throw new IllegalTestException(e.getMessage(), e);
            }

            return constructor;
        }

        private Map<Method, Probability> loadProbabilities() {
            Map<Method, Probability> probMap = new HashMap<>();

            Method defaultMethod = null;
            Probability totalProbability = new Probability(0);

            for (Method method : timeStepMethods) {
                Probability timeStepProbability = loadProbability(method);
                if (timeStepProbability.isMinusOne()) {
                    if (defaultMethod != null) {
                        throw new IllegalTestException("TimeStep method '" + method + "' can't have probability -1."
                                + " Method '" + defaultMethod + "' already has probability -1 and "
                                + "only one such method is allowed");
                    }
                    defaultMethod = method;
                } else {
                    totalProbability = totalProbability.add(timeStepProbability);
                    probMap.put(method, timeStepProbability);
                }
            }

            ensureNotExceedingOne(probMap, totalProbability);

            if (defaultMethod != null) {
                Probability probability = new Probability(1).sub(totalProbability);
                probMap.put(defaultMethod, probability);
            } else if (totalProbability.isSmallerThanOne()) {
                StringBuilder message = new StringBuilder("The total probability of TimeStep methods in test "
                        + testClass.getName() + " is smaller than 1.0, found: " + totalProbability);
                for (Map.Entry<Method, Probability> entry : probMap.entrySet()) {
                    message.append("\n\t").append(entry.getKey()).append(" ").append(entry.getValue());
                }
                throw new IllegalTestException(message.toString());
            }

            return probMap;
        }

        private void ensureNotExceedingOne(Map<Method, Probability> probMap, Probability totalProbability) {
            if (totalProbability.isLargerThanOne()) {
                StringBuilder message = new StringBuilder("Test " + testClass.getName() + " has a total timestep probability of "
                        + totalProbability + " which exceeds 1.0.");
                for (Map.Entry<Method, Probability> entry : probMap.entrySet()) {
                    Method method = entry.getKey();
                    Probability probability = entry.getValue();
                    if (probability.isLargerThanZero()) {
                        message.append("\n\t").append(method).append(" probability=").append(probability);
                    }
                }
                throw new IllegalTestException(message.toString());
            }
        }

        private Probability loadProbability(Method method) {
            String propertyName = method.getName() + PROB;
            String valueString = propertyBinding.load(propertyName);

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

            Probability probability = new Probability(value);

            if (probability.isLargerThanOne()) {
                throw new IllegalTestException("TimeStep method '" + method + "'"
                        + " can't have a probability larger than 1, found: " + probability);
            } else if (probability.isSmallerThanZero() && !probability.isMinusOne()) {
                throw new IllegalTestException("TimeStep method '" + method + "'"
                        + " can't have a probability smaller than 0, found: " + probability);
            }
            return probability;
        }
    }
}
