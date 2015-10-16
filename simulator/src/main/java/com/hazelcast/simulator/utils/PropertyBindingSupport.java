package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.probes.Probe;
import com.hazelcast.simulator.test.TestCase;
import org.apache.log4j.Logger;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.simulator.utils.ReflectionUtils.getFieldValueInternal;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isFinal;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

/**
 * Contains support functionality for binding properties.
 */
public final class PropertyBindingSupport {

    private static final Logger LOGGER = Logger.getLogger(PropertyBindingSupport.class);

    private PropertyBindingSupport() {
    }

    /**
     * Binds all the properties contained in the testCase object onto the test instance.
     *
     * @param testInstance       Instance of the test class
     * @param testCase           TestCase which contains the properties
     * @param optionalProperties Set of optional properties which will not cause a failure if not found
     */
    public static void bindProperties(Object testInstance, TestCase testCase, Set<String> optionalProperties) {
        for (Map.Entry<String, String> entry : testCase.getProperties().entrySet()) {
            String property = entry.getKey();
            String value = entry.getValue();

            // we ignore the class property
            if ("class".equals(property)) {
                continue;
            }

            bindProperty(testInstance, property, value, optionalProperties);
        }
    }

    /**
     * Binds a single property contained in the testCase object onto the object instance.
     *
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
            LOGGER.warn("Optional property could not be bound", e);
        }
    }

    static void bindProperty(Object object, String property, String value) {
        bindProperty(object, property, value, null);
    }

    private static void bindProperty(Object object, String property, String value, Set<String> optionalProperties) {
        value = value.trim();

        String[] path = property.split("\\.");

        object = findPropertyObjectInPath(object, property, path);

        Field field = findPropertyField(object.getClass(), path[path.length - 1]);
        if (field == null) {
            if (optionalProperties != null && optionalProperties.contains(property)) {
                // the property is optional so we just ignore that we couldn't bind it
                return;
            }
            throw new BindException(format("Property [%s.%s] does not exist", object.getClass().getName(), property));
        }

        try {
            if (isProbeField(field) || setValue(object, value, field)) {
                return;
            }
        } catch (Exception e) {
            throw new BindException(format("Failed to bind value [%s] to property [%s.%s] of type [%s]",
                    value, object.getClass().getName(), property, field.getType()), e);
        }

        throw new BindException(format("Unhandled type [%s] for field [%s.%s]",
                field.getType(), object.getClass().getName(), field.getName()));
    }

    private static Object findPropertyObjectInPath(Object object, String property, String[] path) {
        Field field;
        for (int i = 0; i < path.length - 1; i++) {
            Class<?> clazz = object.getClass();
            String element = path[i];
            field = findPropertyField(clazz, element);
            if (field == null) {
                throw new BindException(format("Failed to find field [%s] in property [%s]", element, property));
            }
            object = getFieldValueInternal(object, field, clazz.getName(), property);
            if (object == null) {
                throw new BindException(format("Failed to bind to property [%s] encountered a null value at field [%s]",
                        property, field));
            }
        }
        return object;
    }

    private static Field findPropertyField(Class clazz, String property) {
        try {
            Field field = clazz.getDeclaredField(property);
            if (!isPublic(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is not public", clazz.getName(), property));
            }
            if (isStatic(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is static", clazz.getName(), property));
            }
            if (isFinal(field.getModifiers())) {
                throw new BindException(format("Property [%s.%s] is final", clazz.getName(), property));
            }
            return field;
        } catch (NoSuchFieldException e) {
            Class superClass = clazz.getSuperclass();
            if (superClass == null) {
                return null;
            }
            return findPropertyField(superClass, property);
        }
    }

    private static boolean isProbeField(Field field) {
        return Probe.class.equals(field.getType());
    }

    private static boolean setValue(Object object, String value, Field field) throws IllegalAccessException {
        if (setIntegralValue(object, value, field)) {
            return true;
        }
        if (setFloatingPointValue(object, value, field)) {
            return true;
        }
        return setNonNumericValue(object, value, field);
    }

    private static boolean setIntegralValue(Object object, String value, Field field) throws IllegalAccessException {
        if (Byte.TYPE.equals(field.getType())) {
            // primitive byte
            field.set(object, Byte.parseByte(value));
        } else if (Byte.class.equals(field.getType())) {
            bindByte(object, value, field);
        } else if (Short.TYPE.equals(field.getType())) {
            // primitive short
            field.set(object, Short.parseShort(value));
        } else if (Short.class.equals(field.getType())) {
            bindShort(object, value, field);
        } else if (Integer.TYPE.equals(field.getType())) {
            // primitive integer
            field.set(object, Integer.parseInt(value));
        } else if (Integer.class.equals(field.getType())) {
            bindInteger(object, value, field);
        } else if (Long.TYPE.equals(field.getType())) {
            // primitive long
            field.set(object, Long.parseLong(value));
        } else if (Long.class.equals(field.getType())) {
            bindLong(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static boolean setFloatingPointValue(Object object, String value, Field field) throws IllegalAccessException {
        if (Float.TYPE.equals(field.getType())) {
            // primitive float
            field.set(object, Float.parseFloat(value));
        } else if (Float.class.equals(field.getType())) {
            bindFloat(object, value, field);
        } else if (Double.TYPE.equals(field.getType())) {
            // primitive double
            field.set(object, Double.parseDouble(value));
        } else if (Double.class.equals(field.getType())) {
            bindDouble(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static boolean setNonNumericValue(Object object, String value, Field field) throws IllegalAccessException {
        if (Boolean.TYPE.equals(field.getType())) {
            bindPrimitiveBoolean(object, value, field);
        } else if (Boolean.class.equals(field.getType())) {
            bindBoolean(object, value, field);
        } else if (field.getType().isAssignableFrom(Class.class)) {
            bindClass(object, value, field);
        } else if (String.class.equals(field.getType())) {
            bindString(object, value, field);
        } else if (field.getType().isEnum()) {
            bindEnum(object, value, field);
        } else {
            return false;
        }
        return true;
    }

    private static void bindByte(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Byte.parseByte(value));
        }
    }

    private static void bindShort(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Short.parseShort(value));
        }
    }

    private static void bindInteger(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Integer.parseInt(value));
        }
    }

    private static void bindLong(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Long.parseLong(value));
        }
    }

    private static void bindFloat(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Float.parseFloat(value));
        }
    }

    private static void bindDouble(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, Double.parseDouble(value));
        }
    }

    private static void bindPrimitiveBoolean(Object object, String value, Field field) throws IllegalAccessException {
        if ("true".equals(value)) {
            field.set(object, true);
        } else if ("false".equals(value)) {
            field.set(object, false);
        } else {
            throw new NumberFormatException(format("Unrecognized boolean value: %s", value));
        }
    }

    private static void bindBoolean(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else if ("true".equals(value)) {
            field.set(object, true);
        } else if ("false".equals(value)) {
            field.set(object, false);
        } else {
            throw new NumberFormatException(format("Unrecognized Boolean value: %s", value));
        }
    }

    private static void bindClass(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            try {
                field.set(object, PropertyBindingSupport.class.getClassLoader().loadClass(value));
            } catch (Exception e) {
                throw new BindException(format("Exception while binding class to field %s", field), e);
            }
        }
    }

    private static void bindString(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
            field.set(object, null);
        } else {
            field.set(object, value);
        }
    }

    private static void bindEnum(Object object, String value, Field field) throws IllegalAccessException {
        if ("null".equals(value)) {
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
