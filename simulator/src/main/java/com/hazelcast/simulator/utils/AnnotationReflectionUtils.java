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
package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.InjectMetronome;
import com.hazelcast.simulator.test.annotations.InjectProbe;
import com.hazelcast.simulator.worker.metronome.MetronomeType;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;
import static java.lang.String.format;

public final class AnnotationReflectionUtils {

    static final AnnotationFilter.AlwaysFilter ALWAYS_FILTER = new AnnotationFilter.AlwaysFilter();

    private AnnotationReflectionUtils() {
    }

    public static String getProbeName(Field field) {
        if (field == null) {
            return null;
        }

        InjectProbe injectProbe = field.getAnnotation(InjectProbe.class);
        if (injectProbe != null && !InjectProbe.NULL.equals(injectProbe.name())) {
            return injectProbe.name();
        }
        return field.getName();
    }

    public static boolean isPartOfTotalThroughput(Field field) {
        if (field == null) {
            return false;
        }

        InjectProbe injectProbe = field.getAnnotation(InjectProbe.class);
        if (injectProbe != null) {
            return injectProbe.useForThroughput();
        }
        return false;
    }

    public static int getMetronomeIntervalMillis(Field field, int defaultValue) {
        if (field == null) {
            return defaultValue;
        }

        InjectMetronome injectMetronome = field.getAnnotation(InjectMetronome.class);
        if (injectMetronome != null && injectMetronome.intervalMillis() > 0) {
            return injectMetronome.intervalMillis();
        }
        return defaultValue;
    }

    public static MetronomeType getMetronomeType(Field field, MetronomeType defaultValue) {
        if (field == null) {
            return defaultValue;
        }

        InjectMetronome injectMetronome = field.getAnnotation(InjectMetronome.class);
        if (injectMetronome != null && injectMetronome.type() != NOP) {
            return injectMetronome.type();
        }
        return defaultValue;
    }

    /**
     * Searches for an optional void method of the given annotation type and skips the arguments check.
     *
     * @param classType      Class to scan
     * @param annotationType Type of the annotation
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getAtMostOneVoidMethodSkipArgsCheck(Class classType, Class<? extends Annotation> annotationType) {
        return getAtMostOneMethod(classType, annotationType, ALWAYS_FILTER, null, true);
    }

    /**
     * Searches for an optional void method without arguments of the given annotation type.
     *
     * @param classType      Class to scan
     * @param annotationType Type of the annotation
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getAtMostOneVoidMethodWithoutArgs(Class classType, Class<? extends Annotation> annotationType) {
        return getAtMostOneMethod(classType, annotationType, ALWAYS_FILTER, null, false);
    }

    /**
     * Searches for an optional void method without arguments of the given annotation type and custom annotation filter.
     *
     * @param classType      Class to scan
     * @param annotationType Type of the annotation
     * @param filter         {@link AnnotationFilter} to filter by annotation values
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getAtMostOneVoidMethodWithoutArgs(Class classType, Class<? extends Annotation> annotationType,
                                                           AnnotationFilter filter) {
        return getAtMostOneMethod(classType, annotationType, filter, null, false);
    }

    /**
     * Searches for an optional method without arguments of the given annotation type with a custom return type.
     *
     * @param classType      Class to scan
     * @param annotationType Type of the annotation
     * @param returnType     Assert the return type of the method, use <tt>null</tt> for void methods
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getAtMostOneMethodWithoutArgs(Class classType, Class<? extends Annotation> annotationType,
                                                       Class returnType) {
        return getAtMostOneMethod(classType, annotationType, ALWAYS_FILTER, returnType, false);
    }

    /**
     * Searches for an optional method of the given annotation type.
     *
     * @param classType      Class to scan
     * @param annotationType Type of the annotation
     * @param filter         {@link AnnotationFilter} to filter by annotation values
     * @param returnType     Assert the return type of the method, use <tt>null</tt> for void methods
     * @param skipArgsCheck  set <tt>true</tt> if assertNoArgs should be skipped
     * @return the found method or <tt>null</tt> if no method was found
     */
    private static Method getAtMostOneMethod(Class classType, Class<? extends Annotation> annotationType, AnnotationFilter filter,
                                             Class returnType, boolean skipArgsCheck) {
        List<Method> methods = findMethod(classType, annotationType, filter);
        if (methods == null) {
            return null;
        }
        assertAtMostOne(methods, classType, annotationType);

        Method method = methods.get(0);
        method.setAccessible(true);
        assertNotStatic(method);
        if (returnType == null) {
            assertVoidReturnType(classType, method);
        } else {
            assertReturnType(classType, method, returnType);
        }
        if (!skipArgsCheck) {
            assertNoArgs(method);
        }

        return method;
    }

    /**
     * Returns a list of annotated methods in a class hierarchy.
     *
     * Returns more than one method only if they are declared in the same class.
     * As soon as at least one method has been found, no super class will be searched.
     * So a child class will always overwrite the annotated methods from its superclass.
     *
     * @param annotation Type of the annotation to search for
     * @param filter     Filter to filter search result by annotation values
     * @return List of found methods with this annotation or <tt>null</tt> if no methods were found
     */
    private static List<Method> findMethod(Class classType, Class<? extends Annotation> annotation, AnnotationFilter filter) {
        List<Method> methods = new LinkedList<Method>();
        do {
            findMethod(classType, annotation, filter, methods);
            if (!methods.isEmpty()) {
                return methods;
            }
            classType = classType.getSuperclass();
        } while (classType != null);

        return null;
    }

    @SuppressWarnings("unchecked")
    private static void findMethod(Class searchClass, Class<? extends Annotation> annotation, AnnotationFilter filter,
                                   List<Method> methods) {
        for (Method method : searchClass.getDeclaredMethods()) {
            Annotation found = method.getAnnotation(annotation);
            if (found != null && filter.allowed(found)) {
                methods.add(method);
            }
        }
    }

    private static void assertAtMostOne(List<Method> methods, Class classType, Class<? extends Annotation> annotation) {
        if (methods.size() > 1) {
            throw new ReflectionException(format("Too many methods on class %s with annotation %s", classType.getName(),
                    annotation.getName()));
        }
    }

    private static void assertNotStatic(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new ReflectionException(format("Method  %s can't be static", method.getName()));
        }
    }

    private static void assertVoidReturnType(Class classType, Method method) {
        assertReturnType(classType, method, Void.TYPE);
    }

    private static void assertReturnType(Class classType, Method method, Class<?> returnType) {
        if (returnType.isAssignableFrom(method.getReturnType())) {
            return;
        }
        throw new ReflectionException(format("Method %s.%s should have returnType %s", classType, method, returnType));
    }

    private static void assertNoArgs(Method method) {
        if (method.getParameterTypes().length == 0) {
            return;
        }

        throw new ReflectionException(format("Method '%s' can't have any args", method));
    }
}
