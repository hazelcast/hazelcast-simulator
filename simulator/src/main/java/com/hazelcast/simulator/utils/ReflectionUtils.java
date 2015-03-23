package com.hazelcast.simulator.utils;

import com.hazelcast.simulator.test.annotations.Name;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public final class ReflectionUtils {

    private static final AnnotationFilter.AlwaysFilter ALWAYS_FILTER = new AnnotationFilter.AlwaysFilter();

    private ReflectionUtils() {
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

    public static Field getField(Class searchClass, String fieldName, Class fieldType) {
        for (Field field : searchClass.getDeclaredFields()) {
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

    public static String getValueFromNameAnnotation(Field field) {
        Name nameAnnotation = field.getAnnotation(Name.class);
        if (nameAnnotation != null) {
            return nameAnnotation.value();
        }
        return field.getName();
    }

    public static String getValueFromNameAnnotations(Annotation[] annotations, String defaultName) {
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().equals(Name.class)) {
                Name name = (Name) annotation;
                return name.value();
            }
        }
        return defaultName;
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
        assertAtMostOne(methods, classType, annotationType);
        if (methods.isEmpty()) {
            return null;
        }

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
     * @return List of found methods with this annotation
     */
    private static List<Method> findMethod(Class classType, Class<? extends Annotation> annotation, AnnotationFilter filter) {
        List<Method> methods = new LinkedList<Method>();

        // search in base class
        findMethod(classType, annotation, filter, methods);

        Class searchClass = classType;
        while (methods.size() == 0 && searchClass.getSuperclass() != null) {
            // search in super class
            searchClass = searchClass.getSuperclass();
            findMethod(searchClass, annotation, filter, methods);
        }

        return methods;
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
            throw new RuntimeException(
                    format("Too many methods on class %s with annotation %s", classType.getName(), annotation.getName()));
        }
    }

    private static void assertNotStatic(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new RuntimeException(format("Method  %s can't be static", method.getName()));
        }
    }

    private static void assertVoidReturnType(Class classType, Method method) {
        assertReturnType(classType, method, Void.TYPE);
    }

    private static void assertReturnType(Class classType, Method method, Class returnType) {
        if (returnType.equals(method.getReturnType())) {
            return;
        }
        throw new RuntimeException(format("Method %s.%s should have returnType %s", classType, method, returnType));
    }

    private static void assertNoArgs(Method method) {
        if (method.getParameterTypes().length == 0) {
            return;
        }

        throw new RuntimeException(format("Method '%s' can't have any args", method));
    }
}
