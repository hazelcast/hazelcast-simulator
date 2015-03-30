/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

public final class ReflectionUtils {

    private ReflectionUtils() {
    }

    public static <E> void invokePrivateConstructor(Class<E> classType) throws Exception {
        Constructor<E> constructor = classType.getDeclaredConstructor();
        constructor.setAccessible(true);
        constructor.newInstance();
    }

    @SuppressWarnings("unchecked")
    public static <E> E invokeMethod(Object classInstance, Method method, Object... args) throws Throwable {
        if (method == null) {
            return null;
        }

        try {
            return (E) method.invoke(classInstance, args);
        } catch (InvocationTargetException e) {
            throw e.getCause();
        }
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

    private static Field findField(Class classType, String fieldName, Class fieldType) {
        for (Field field : classType.getDeclaredFields()) {
            if (field.getName().equals(fieldName) && ((fieldType != null && field.getType().isAssignableFrom(fieldType)) || (
                    fieldType == null && field.getType().isPrimitive()))) {
                return field;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    public static <E> E getObjectFromField(Object object, String fieldName) {
        if (object == null) {
            throw new NullPointerException("Object to retrieve field from can't be null");
        }

        try {
            Field field = object.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(object);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void injectObjectToInstance(Object classInstance, Field field, Object object) {
        field.setAccessible(true);
        try {
            field.set(classInstance, object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Searches a method by name.
     *
     * @param classType  Class to scan
     * @param methodName Name of the method
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getMethodByName(Class classType, String methodName) {
        for (Method method : classType.getDeclaredMethods()) {
            if (method.getName().equals(methodName)) {
                return method;
            }
        }
        return null;
    }
}
