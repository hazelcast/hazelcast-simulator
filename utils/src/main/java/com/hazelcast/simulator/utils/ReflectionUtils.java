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
package com.hazelcast.simulator.utils;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import static java.lang.String.format;

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static Field getField(Class classType, String fieldName, Class fieldType) {
        Field field;
        do {
            field = findField(classType, fieldName, fieldType);
            if (field != null) {
                return field;
            }
            classType = classType.getSuperclass();
        } while (classType != null);

        return null;
    }

    public static void setFieldValue(Object instance, Field field, Object value) {
        field.setAccessible(true);
        setFieldValueInternal(instance, field, value);
    }

    public static <E> E getFieldValue(Object instance, String fieldName) {
        if (instance == null) {
            throw new NullPointerException("Object to retrieve field from can't be null");
        }

        Field field;
        Class<?> clazz = instance.getClass();
        try {
            field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
        } catch (Exception e) {
            throw new ReflectionException(e);
        }
        return getFieldValueInternal(instance, field, clazz.getName(), fieldName);
    }

    /**
     * Gets the value for a static field.
     *
     * @param clazz     class which contains the field
     * @param fieldName name of the field
     * @param fieldType type of the field
     * @return the value of the static field
     */
    public static <E> E getStaticFieldValue(Class clazz, String fieldName, Class fieldType) {
        Field field = getField(clazz, fieldName, fieldType);
        if (field == null) {
            throw new ReflectionException(format("Field %s.%s is not found", clazz.getName(), fieldName));
        }

        field.setAccessible(true);
        return getFieldValueInternal(null, field, clazz.getName(), fieldName);
    }

    /**
     * Searches a method by name.
     *
     * @param clazz      Class to scan
     * @param methodName Name of the method
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getMethodByName(Class clazz, String methodName) {
        for (Method method : clazz.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <E> E invokeMethod(Object classInstance, Method method, Object... args) throws Exception {
        if (method == null) {
            return null;
        }

        try {
            return (E) method.invoke(classInstance, args);
        } catch (InvocationTargetException e) {
            if (e.getCause() instanceof Error) {
                throw (Error) e.getCause();
            }
            throw (Exception) e.getCause();
        }
    }

    public static <E> void invokePrivateConstructor(Class<E> classType) throws Exception {
        Constructor<E> constructor = classType.getDeclaredConstructor();
        constructor.setAccessible(true);
        constructor.newInstance();
    }

    static void setFieldValueInternal(Object instance, Field field, Object value) {
        try {
            field.set(instance, value);
        } catch (IllegalAccessException e) {
            throw new ReflectionException(e);
        }
    }

    @SuppressWarnings("unchecked")
    static <E> E getFieldValueInternal(Object instance, Field field, String className, String fieldName) {
        try {
            return (E) field.get(instance);
        } catch (IllegalAccessException e) {
            throw new ReflectionException(format("Failed to access %s.%s", className, fieldName), e);
        }
    }

    private static Field findField(Class classType, String fieldName, Class fieldType) {
        for (Field field : classType.getDeclaredFields()) {
            if (field.getName().equals(fieldName)) {
                boolean isAssignableToType = (fieldType != null && field.getType().isAssignableFrom(fieldType));
                boolean isPrimitiveType = (fieldType == null && field.getType().isPrimitive());
                if (isAssignableToType || isPrimitiveType) {
                    return field;
                }
            }
        }
        return null;
    }
}
