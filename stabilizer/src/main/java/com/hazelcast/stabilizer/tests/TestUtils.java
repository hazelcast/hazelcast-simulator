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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.stabilizer.Utils.getFile;
import static com.hazelcast.stabilizer.Utils.writeObject;
import static java.lang.String.format;

public class TestUtils {

    public static Node getNode(HazelcastInstance hz) {
        HazelcastInstanceImpl impl = getHazelcastInstanceImpl(hz);
        return impl != null ? impl.node : null;
    }

    public static long secondsToMillis(int seconds){
        return seconds*1000;
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

    public static <E> E getField(Object o, String fieldName){
        try {
            Field field = o.getClass().getDeclaredField(fieldName);
            field.setAccessible(true);
            return (E)field.get(o);
        }catch(NoSuchFieldException e){
            throw new RuntimeException(e);
        }catch(IllegalAccessException e){
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
            if (Boolean.class.equals(field.getType()) || Boolean.TYPE.equals(field.getType())) {
                field.set(test, Boolean.parseBoolean(value));
            } else if (String.class.equals(field.getType())) {
                field.set(test, value);
            } else if (Integer.class.equals(field.getType()) || Integer.TYPE.equals(field.getType())) {
                field.set(test, Integer.parseInt(value));
            } else if (Long.class.equals(field.getType()) || Long.TYPE.equals(field.getType())) {
                field.set(test, Long.parseLong(value));
            } else if (Float.class.equals(field.getType()) || Float.TYPE.equals(field.getType())) {
                field.set(test, Float.parseFloat(value));
            } else if (Double.class.equals(field.getType()) || Double.TYPE.equals(field.getType())) {
                field.set(test, Double.parseDouble(value));
            } else {
                throw new RuntimeException(
                        format("Can't bind property [%s] to field of type [%s]", property, field.getType()));
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
