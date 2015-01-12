package com.hazelcast.stabilizer.test.utils;

import com.hazelcast.stabilizer.test.TestCase;
import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.test.exceptions.BindException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;

import static java.lang.String.format;

/**
 * Contains support functionality for binding properties.
 */
public final class PropertyBindingSupport {


    public static <E> E getField(Object object, String fieldName) {
        if (object == null) {
            throw new NullPointerException("object can't be null");
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

    /**
     * Binds all the properties contained in the testCase object onto the test object.
     *
     * @param test
     * @param testCase
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public static void bindProperties(Object test, TestCase testCase) throws NoSuchFieldException, IllegalAccessException {
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            String value = entry.getValue();

            // we ignore the class property
            if ("class".equals(property)) {
                continue;
            }

            //we also ignore probe- properties
            if (property.startsWith("probe-")) {
                continue;
            }

            bindProperty(test, property, value);
        }
    }

    public static void bindProperty(Object object, String property, String value) throws IllegalAccessException {
        String[] path = property.split("\\.");

        Field field;
        for (int k = 0; k < path.length - 1; k++) {
            String element = path[k];
            field = findPropertyField(object.getClass(), element);
            if (field == null) {
                throw new BindException("Failed to find property:" + element + " in property: " + property);
            }
            object = field.get(object);
            if (object == null) {
                throw new BindException("Failed to bind to property: " + property + " encountered a null value at field:" + field);
            }
        }

        field = findPropertyField(object.getClass(), path[path.length - 1]);

        if (field == null) {
            throw new BindException(
                    format("Property [%s.%s] does not exist", object.getClass().getName(), property));
        }

        try {
            if (Boolean.TYPE.equals(field.getType())) {
                //primitive boolean
                if ("true".equals(value)) {
                    field.set(object, true);
                } else if ("false".equals(value)) {
                    field.set(object, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Boolean.class.equals(field.getType())) {
                //object boolean
                if ("null".equals(value)) {
                    field.set(object, null);
                } else if ("true".equals(value)) {
                    field.set(object, true);
                } else if ("false".equals(value)) {
                    field.set(object, false);
                } else {
                    throw new NumberFormatException("Unrecognized boolean value:" + value);
                }
            } else if (Byte.TYPE.equals(field.getType())) {
                //primitive byte
                field.set(object, Byte.parseByte(value));
            } else if (Byte.class.equals(field.getType())) {
                //object byte
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Byte.parseByte(value));
                }
            } else if (Short.TYPE.equals(field.getType())) {
                //primitive short
                field.set(object, Short.parseShort(value));
            } else if (Short.class.equals(field.getType())) {
                //object short
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Short.parseShort(value));
                }
            } else if (Integer.TYPE.equals(field.getType())) {
                //primitive integer
                field.set(object, Integer.parseInt(value));
            } else if (Integer.class.equals(field.getType())) {
                //object integer
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Integer.parseInt(value));
                }
            } else if (Long.TYPE.equals(field.getType())) {
                //primitive long
                field.set(object, Long.parseLong(value));
            } else if (Long.class.equals(field.getType())) {
                //object long
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Long.parseLong(value));
                }
            } else if (Float.TYPE.equals(field.getType())) {
                //primitive float
                field.set(object, Float.parseFloat(value));
            } else if (Float.class.equals(field.getType())) {
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Float.parseFloat(value));
                }
            } else if (Double.TYPE.equals(field.getType())) {
                //primitive double
                field.set(object, Double.parseDouble(value));
            } else if (Double.class.equals(field.getType())) {
                //object double
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Double.parseDouble(value));
                }
            }else if (field.getType().isAssignableFrom(Class.class)) {
                // class
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, PropertyBindingSupport.class.getClassLoader().loadClass(value));
                }
            } else if (String.class.equals(field.getType())) {
                //string
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, value);
                }
            } else if (field.getType().isEnum()) {
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    try {
                        Object enumValue = getEnumValue(value, field);
                        field.set(object, enumValue);
                    } catch (IllegalArgumentException e) {
                        throw new NumberFormatException(e.getMessage());
                    }
                }
            } else {
                throw new BindException(
                        format("Unhandled type [%s] for field [%s.%s]", field.getType(), object.getClass().getName(), field.getName()));
            }
        } catch (BindException e) {
            throw e;
        } catch (Exception e) {
            throw new BindException(
                    format("Failed to bind value [%s] to property [%s.%s] of type [%s]",
                            value, object.getClass().getName(), property, field.getType())
            );
        }
    }

    private static Enum getEnumValue(String value, Field field) throws Exception {
        Class<? extends Enum> type = (Class<? extends Enum>) field.getType();
        Method method = type.getMethod("values");
        Enum[] values = (Enum[]) method.invoke(null);

        for (Enum v : values) {
            if (v.name().equalsIgnoreCase(value)) {
                return v;
            }
        }

        throw new RuntimeException("Could not find enum value " + type.getSimpleName() + "." + value);
    }

    public static Field findPropertyField(Class clazz, String property) {
        try {
            Field field = clazz.getDeclaredField(property);

            if (Modifier.isStatic(field.getModifiers())) {
                throw new BindException(
                        format("Property [%s.%s] can't be static", clazz.getName(), property));
            }

            if (Modifier.isFinal(field.getModifiers())) {
                throw new BindException(
                        format("Property [%s.%s] can't be final", clazz.getName(), property));
            }

            field.setAccessible(true);
            return field;
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                return null;
            } else {
                return findPropertyField(superClass, property);
            }
        }
    }

    public static ProbesConfiguration parseProbeConfiguration(TestCase testCase) {
        ProbesConfiguration configuration = new ProbesConfiguration();
        String probePrefix = "probe-";
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            if (property.startsWith(probePrefix)) {
                String probeName = property.substring(probePrefix.length());
                configuration.addConfig(probeName, entry.getValue());
            }
        }
        return configuration;
    }
}
