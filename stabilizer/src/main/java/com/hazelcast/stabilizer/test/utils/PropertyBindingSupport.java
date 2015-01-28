package com.hazelcast.stabilizer.test.utils;

import com.hazelcast.stabilizer.probes.probes.ProbesConfiguration;
import com.hazelcast.stabilizer.test.TestCase;
import com.hazelcast.stabilizer.test.exceptions.BindException;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import java.util.Set;

import static java.lang.String.format;

/**
 * Contains support functionality for binding properties.
 */
public final class PropertyBindingSupport {

    private static final Logger log = Logger.getLogger(PropertyBindingSupport.class);

    /**
     * Binds all the properties contained in the testCase object onto the test instance.
     *
     * @param testInstance Instance of the test class
     * @param testCase     TestCase which contains the properties
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     */
    public static void bindProperties(Object testInstance, TestCase testCase, Set<String> optionalProperties)
            throws NoSuchFieldException, IllegalAccessException {
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            String value = entry.getValue();

            // we ignore the class property
            if ("class".equals(property)) {
                continue;
            }

            // we also ignore probe-properties
            if (property.startsWith("probe-")) {
                continue;
            }

            bindProperty(testInstance, property, value, optionalProperties);
        }
    }

    /**
     * Binds a single property contained in the testCase object onto the object instance.
     * <p/>
     * There will be no warning if the property is not defined in the TestCase.
     * There will be no exception if the property will not be found in the object instance, just a warning.
     *
     * @param instance     Instance were the property should be injected
     * @param testCase     TestCase which contains
     * @param propertyName Name of the property which should be injected
     */
    public static void bindOptionalProperty(Object instance, TestCase testCase, String propertyName) {
        if (testCase == null) {
            return;
        }

        String propertyValue = testCase.getProperty(propertyName);
        if (propertyValue == null || propertyValue.isEmpty()) {
            return;
        }

        try {
            bindProperty(instance, propertyName, propertyValue);
        } catch (Exception e) {
            log.warn("Optional property could not be bound", e);
        }
    }

    static void bindProperty(Object object, String property, String value) throws IllegalAccessException {
        bindProperty(object, property, value, null);
    }

    private static void bindProperty(Object object, String property, String value, Set<String> optionalProperties)
            throws IllegalAccessException {
        String[] path = property.split("\\.");

        Field field;
        for (int i = 0; i < path.length - 1; i++) {
            String element = path[i];
            field = findPropertyField(object.getClass(), element);
            if (field == null) {
                throw new BindException(format("Failed to find property: %s in property: %s", element, property));
            }
            object = field.get(object);
            if (object == null) {
                throw new BindException(
                        format("Failed to bind to property: %s encountered a null value at field: %s", property, field));
            }
        }

        field = findPropertyField(object.getClass(), path[path.length - 1]);
        if (field == null) {
            if (optionalProperties != null && optionalProperties.contains(property)) {
                return;
            }
            throw new BindException(format("Property [%s.%s] does not exist", object.getClass().getName(), property));
        }

        try {
            if (Boolean.TYPE.equals(field.getType())) {
                // primitive boolean
                if ("true".equals(value)) {
                    field.set(object, true);
                } else if ("false".equals(value)) {
                    field.set(object, false);
                } else {
                    throw new NumberFormatException(format("Unrecognized boolean value: %b", value));
                }
            } else if (Boolean.class.equals(field.getType())) {
                // object boolean
                if ("null".equals(value)) {
                    field.set(object, null);
                } else if ("true".equals(value)) {
                    field.set(object, true);
                } else if ("false".equals(value)) {
                    field.set(object, false);
                } else {
                    throw new NumberFormatException(format("Unrecognized Boolean value: %b", value));
                }
            } else if (Byte.TYPE.equals(field.getType())) {
                // primitive byte
                field.set(object, Byte.parseByte(value));
            } else if (Byte.class.equals(field.getType())) {
                // object byte
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Byte.parseByte(value));
                }
            } else if (Short.TYPE.equals(field.getType())) {
                // primitive short
                field.set(object, Short.parseShort(value));
            } else if (Short.class.equals(field.getType())) {
                // object short
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Short.parseShort(value));
                }
            } else if (Integer.TYPE.equals(field.getType())) {
                // primitive integer
                field.set(object, Integer.parseInt(value));
            } else if (Integer.class.equals(field.getType())) {
                // object integer
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Integer.parseInt(value));
                }
            } else if (Long.TYPE.equals(field.getType())) {
                // primitive long
                field.set(object, Long.parseLong(value));
            } else if (Long.class.equals(field.getType())) {
                // object long
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Long.parseLong(value));
                }
            } else if (Float.TYPE.equals(field.getType())) {
                // primitive float
                field.set(object, Float.parseFloat(value));
            } else if (Float.class.equals(field.getType())) {
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Float.parseFloat(value));
                }
            } else if (Double.TYPE.equals(field.getType())) {
                // primitive double
                field.set(object, Double.parseDouble(value));
            } else if (Double.class.equals(field.getType())) {
                // object double
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, Double.parseDouble(value));
                }
            } else if (field.getType().isAssignableFrom(Class.class)) {
                // class
                if ("null".equals(value)) {
                    field.set(object, null);
                } else {
                    field.set(object, PropertyBindingSupport.class.getClassLoader().loadClass(value));
                }
            } else if (String.class.equals(field.getType())) {
                // string
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
                        format("Unhandled type [%s] for field [%s.%s]", field.getType(), object.getClass().getName(),
                                field.getName()));
            }
        } catch (BindException e) {
            throw e;
        } catch (Exception e) {
            throw new BindException(
                    format("Failed to bind value [%s] to property [%s.%s] of type [%s]", value, object.getClass().getName(),
                            property, field.getType()));
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

        throw new RuntimeException(format("Could not find enum value %s.%s", type.getSimpleName(), value));
    }

    private static Field findPropertyField(Class clazz, String property) {
        try {
            Field field = clazz.getDeclaredField(property);

            if (Modifier.isStatic(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] can't be static", clazz.getName(), property));
            }

            if (Modifier.isFinal(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] can't be final", clazz.getName(), property));
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
