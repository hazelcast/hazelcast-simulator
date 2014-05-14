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
package com.hazelcast.stabilizer.tests;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.HazelcastInstanceProxy;
import com.hazelcast.instance.Node;
import com.hazelcast.stabilizer.TestCase;

import java.lang.reflect.Field;
import java.util.Map;

import static java.lang.String.format;

public class TestUtils {

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static long secondsToMillis(int seconds) {
        return seconds * 1000;
    }


    public static HazelcastInstanceImpl getHazelcastInstanceImpl(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = null;
        if (hz instanceof HazelcastInstanceProxy) {
            return getField(hz, "original");
        } else if (hz instanceof HazelcastInstanceImpl) {
            impl = (HazelcastInstanceImpl) hz;
        }
        return impl;
    }

    public static <E> E getField(Object o, String fieldName) {
        try {
            Field field = o.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E) field.get(o);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void bindProperties(Test test, TestCase testCase) throws NoSuchFieldException, IllegalAccessException {
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            if ("class".equals(property)) {
                continue;
            }
            String value = entry.getValue();
            bindProperty(test, property, value);
        }
    }

    public static void bindProperty(Test test, String property, String value) throws IllegalAccessException {
        Field field = findField(test.getClass(), property);
        if (field == null) {
            throw new RuntimeException(
                    format("Could not found a field for property [%s] on class [%s]", property, test.getClass()));
        }
        field.setAccessible(true);

        try {
            if (Boolean.TYPE.equals(field.getType())) {
                //primitive boolean
                if ("true".equals(value)) {
                    field.set(test, true);
                } else if ("false".equals(value)) {
                    field.set(test, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Boolean.class.equals(field.getType())) {
                //object boolean
                if ("null".equals(value)) {
                    field.set(test, null);
                } else if ("true".equals(value)) {
                    field.set(test, true);
                } else if ("false".equals(value)) {
                    field.set(test, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Byte.TYPE.equals(field.getType())) {
                //primitive byte
                field.set(test, Byte.parseByte(value));
            } else if (Byte.class.equals(field.getType())) {
                //object byte
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Byte.parseByte(value));
                }
            } else if (Short.TYPE.equals(field.getType())) {
                //primitive short
                field.set(test, Short.parseShort(value));
            } else if (Short.class.equals(field.getType())) {
                //object short
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Short.parseShort(value));
                }
            } else if (Integer.TYPE.equals(field.getType())) {
                //primitive integer
                field.set(test, Integer.parseInt(value));
            } else if (Integer.class.equals(field.getType())) {
                //object integer
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Integer.parseInt(value));
                }
            } else if (Long.TYPE.equals(field.getType())) {
                //primitive long
                field.set(test, Long.parseLong(value));
            } else if (Long.class.equals(field.getType())) {
                //object long
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Long.parseLong(value));
                }
            } else if (Float.TYPE.equals(field.getType())) {
                //primitive float
                field.set(test, Float.parseFloat(value));
            } else if (Float.class.equals(field.getType())) {
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Float.parseFloat(value));
                }
            } else if (Double.TYPE.equals(field.getType())) {
                //primitive double
                field.set(test, Double.parseDouble(value));
            } else if (Double.class.equals(field.getType())) {
                //object double
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, Double.parseDouble(value));
                }
            } else if (String.class.equals(field.getType())) {
                //string
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    field.set(test, value);
                }
            } else if (field.getType().isEnum()) {
                if ("null".equals(value)) {
                    field.set(test, null);
                } else {
                    Object enumValue = Enum.valueOf((Class<? extends Enum>) field.getType(), value);
                    field.set(test, enumValue);
                }
            } else {
                throw new RuntimeException(
                        format("Unhandled type [%s] for field %s.%s", field.getType(), test.getClass().getName(), field.getName()));
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException(
                    format("Failed to convert property [%s] value [%s] to type [%s]", property, value, field.getType()), e);
        }
    }

    public static Field findField(Class clazz, String property) {
        try {
            return clazz.getDeclaredField(property);
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            return superClass != null ? findField(superClass, property) : null;
        }
    }

    private TestUtils() {
    }
}
