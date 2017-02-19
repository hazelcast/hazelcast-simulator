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

import com.hazelcast.simulator.common.TestCase;
import com.hazelcast.simulator.probes.Probe;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValue0;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

/**
 * Contains support functionality for binding properties.
 */
public final class PropertyBindingSupport {

    private static final String NULL_LITERAL = "null";

    private PropertyBindingSupport() {
    }

    /**
     * Binds a single property contained in the {@link TestCase} instance onto the object instance.
     *
     * There will be no warning if the property is not defined in the {@link TestCase}.
     * There will be no exception if the property will not be found in the object instance, just a warning.
     *
     * @param instance     Instance were the property should be injected
     * @param testCase     TestCase which contains
     * @param propertyName Name of the property which should be injected
     * @return true if a matching property was found in the testCase, false otherwise.
     * @throws RuntimeException if there was an error binding
     */
    public static boolean bind(Object instance, TestCase testCase, String propertyName) {
        String value = getValue(testCase, propertyName);
        if (value == null) {
            return false;
        }

        return bind0(instance, propertyName, value);
    }

    public static Set<String> bindAll(Object instance, TestCase testCase) {
        Set<String> usedProperties = new HashSet<String>();

        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String fullPropertyPath = entry.getKey().trim();
            String value = entry.getValue().trim();

            if (bind0(instance, fullPropertyPath, value)) {
                usedProperties.add(fullPropertyPath);
            }
        }

        return usedProperties;
    }

    /**
     * Returns a single property contained in the {@link TestCase} instance.
     *
     * There will be no warning if the property is not defined in the {@link TestCase}.
     *
     * @param testCase     TestCase which contains
     * @param propertyName Name of the property which should be injected
     * @return Value of the property if found, {@code null} otherwise
     */
    private static String getValue(TestCase testCase, String propertyName) {
        if (testCase == null) {
            return null;
        }

        String propertyValue = testCase.getProperty(propertyName);
        if (propertyValue == null || propertyValue.isEmpty()) {
            return null;
        }
        return propertyValue;
    }

    static boolean bind0(Object object, String property, String value) {
        value = value.trim();

        String[] path = property.split("\\.");

        object = findTargetObject(object, property, path);
        if (object == null) {
            return false;
        }

        Field field = findField(object.getClass(), path[path.length - 1]);
        if (field == null || isProbeField(field)) {
            return false;
        }

        try {
            setField(field, object, value);
            return true;
        } catch (Exception e) {
            throw new BindException(format("Failed to bind value [%s] to property [%s.%s] of type [%s]",
                    value, object.getClass().getName(), property, field.getType()), e);
        }
    }

    private static Object findTargetObject(Object parent, String property, String[] path) {
        for (int i = 0; i < path.length - 1; i++) {
            Class<?> clazz = parent.getClass();
            String fieldName = path[i];
            Field field = findField(clazz, fieldName);
            if (field == null) {
                if (i == 0) {
                    // we have no match at all
                    return null;
                } else {
                    // we found at least one item in the path
                    throw new BindException(
                            format("Failed to find field [%s.%s] in property [%s]", clazz.getName(), fieldName, property));
                }
            }

            Object child = getFieldValue0(parent, field, clazz.getName(), property);
            if (child == null) {
                try {
                    child = field.getType().newInstance();
                    field.set(parent, child);
                } catch (InstantiationException e) {
                    throw new BindException(format("Failed to initialize null field '%s'", field), e);
                } catch (IllegalAccessException e) {
                    throw new BindException(format("Failed to initialize null field '%s'", field), e);
                }
            }

            parent = child;
        }
        return parent;
    }

    private static Field findField(Class clazz, String fieldName) {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            if (!isPublic(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is not public", clazz.getName(), fieldName));
            }
            if (isStatic(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is static", clazz.getName(), fieldName));
            }
            if (isFinal(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is final", clazz.getName(), fieldName));
            }
            return field;
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                return null;
            }
            return findField(superClass, fieldName);
        }
    }

    private static boolean isProbeField(Field field) {
        return Probe.class.equals(field.getType());
    }

    private static void setField(Field field, Object object, String value) throws IllegalAccessException {
        if (setIntegralField(field, object, value)
                || setFloatingPointField(field, object, value)
                || setNonNumericField(field, object, value)) {
            return;
        }

        throw new BindException(format("Property [%s.%s] has unsupported type '%s'",
                object.getClass().getName(), field.getName(), field.getType()));
    }

    private static boolean setIntegralField(Field field, Object object, String value) throws IllegalAccessException {
        value = removeUnderscores(value);

        if (Byte.TYPE.equals(field.getType())) {
            // primitive byte
            field.set(object, Byte.parseByte(value));
        } else if (Byte.class.equals(field.getType())) {
            setByteField(object, value, field);
        } else if (Short.TYPE.equals(field.getType())) {
            // primitive short
            field.set(object, Short.parseShort(value));
        } else if (Short.class.equals(field.getType())) {
            setShortField(object, value, field);
        } else if (Integer.TYPE.equals(field.getType())) {
            // primitive integer
            field.set(object, Integer.parseInt(value));
        } else if (Integer.class.equals(field.getType())) {
            setIntegerField(object, value, field);
        } else if (Long.TYPE.equals(field.getType())) {
            // primitive long
            field.set(object, Long.parseLong(value));
        } else if (Long.class.equals(field.getType())) {
            setLongField(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static String removeUnderscores(String s) {
        return s.replaceAll("_", "");
    }

    private static boolean setFloatingPointField(Field field, Object object, String value) throws IllegalAccessException {
        value = removeUnderscores(value);

        if (Float.TYPE.equals(field.getType())) {
            // primitive float
            field.set(object, Float.parseFloat(value));
        } else if (Float.class.equals(field.getType())) {
            setFloatField(object, value, field);
        } else if (Double.TYPE.equals(field.getType())) {
            // primitive double
            field.set(object, Double.parseDouble(value));
        } else if (Double.class.equals(field.getType())) {
            setDoubleField(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static boolean setNonNumericField(Field field, Object object, String value) throws IllegalAccessException {
        if (Boolean.TYPE.equals(field.getType())) {
            setPrimitiveBooleanField(object, value, field);
        } else if (Boolean.class.equals(field.getType())) {
            setBooleanField(object, value, field);
        } else if (field.getType().isAssignableFrom(Class.class)) {
            setClassField(object, value, field);
        } else if (String.class.equals(field.getType())) {
            setStringField(object, value, field);
        } else if (field.getType().isEnum()) {
            setEnumField(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static void setByteField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Byte.parseByte(value));
        }
    }

    private static void setShortField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Short.parseShort(value));
        }
    }

    private static void setIntegerField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Integer.parseInt(value));
        }
    }

    private static void setLongField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Long.parseLong(value));
        }
    }

    private static void setFloatField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Float.parseFloat(value));
        }
    }

    private static void setDoubleField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Double.parseDouble(value));
        }
    }

    private static void setPrimitiveBooleanField(Object object, String value, Field field) throws IllegalAccessException {
        if ("true".equals(value)) {
            field.set(object, true);
        } else if ("false".equals(value)) {
            field.set(object, false);
        } else {
            throw new NumberFormatException(format("Unrecognized boolean value: %s", value));
        }
    }

    private static void setBooleanField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else if ("true".equals(value)) {
            field.set(object, true);
        } else if ("false".equals(value)) {
            field.set(object, false);
        } else {
            throw new NumberFormatException(format("Unrecognized Boolean value: %s", value));
        }
    }

    private static void setClassField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            try {
                field.set(object, PropertyBindingSupport.class.getClassLoader().loadClass(value));
            } catch (Exception e) {
                throw new BindException(format("Exception while binding class to field %s", field), e);
            }
        }
    }

    private static void setStringField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, value);
        }
    }

    private static void setEnumField(Object object, String value, Field field) throws IllegalAccessException {
        if (NULL_LITERAL.equals(value)) {
            field.set(object, null);
        } else {
            try {
                Object enumValue = getEnumValue(value, field);
                field.set(object, enumValue);
            } catch (Exception e) {
                throw new BindException(format("Exception while binding Enum to field %s", field), e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Enum getEnumValue(String value, Field field) throws Exception {
        Class<? extends Enum> type = (Class<? extends Enum>) field.getType();
        Method method = type.getMethod("values");
        Enum[] values = (Enum[]) method.invoke(null);

        for (Enum enumValue : values) {
            if (enumValue.name().equalsIgnoreCase(value)) {
                return enumValue;
            }
        }

        throw new BindException(format("Could not find enum value %s.%s", type.getSimpleName(), value));
    }
}
