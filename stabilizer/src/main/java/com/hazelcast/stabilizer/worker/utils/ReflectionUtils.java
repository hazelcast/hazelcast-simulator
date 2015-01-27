package com.hazelcast.stabilizer.worker.utils;

import com.hazelcast.stabilizer.test.exceptions.IllegalTestException;
import com.hazelcast.stabilizer.worker.utils.AnnotationFilter.AlwaysFilter;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.LinkedList;
import java.util.List;

import static java.lang.String.format;

public class ReflectionUtils {

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

    public static void injectObjectToInstance(Object classInstance, Field field, Object object) {
        field.setAccessible(true);
        try {
            field.set(classInstance, object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static Method getMandatoryVoidMethodWithoutArgs(Class classType, Class<? extends Annotation> annotationType) {
        List<Method> methods = findMethod(classType, annotationType);
        assertExactlyOne(methods, classType, annotationType);

        Method method = methods.get(0);
        method.setAccessible(true);
        assertNotStatic(method);
        assertVoidReturnType(classType, method);
        assertNoArgs(method);

        return method;
    }

    public static Method getOptionalVoidMethodSkipArgsCheck(Class<?> classType, Class<? extends Annotation> annotationType) {
        return getOptionalAnnotationMethod(classType, annotationType, new AlwaysFilter(), null, true);
    }

    public static Method getOptionalVoidMethodWithoutArgs(Class<?> classType, Class<? extends Annotation> annotationType,
                                                          AnnotationFilter filter) {
        return getOptionalAnnotationMethod(classType, annotationType, filter, null, false);
    }

    public static Method getOptionalMethodWithoutArgs(Class<?> classType, Class<? extends Annotation> annotationType,
                                                      Class returnType) {
        return getOptionalAnnotationMethod(classType, annotationType, new AlwaysFilter(), returnType, false);
    }

    /**
     * Searches for an optional method of the given annotation.
     *
     * @param classType      Class to scan
     * @param annotation     Type of the annotation
     * @param filter         Filter to filter by annotation values
     * @param returnType     Assert the return type of the method, use <tt>null</tt> for void methods
     * @param skipArgsCheck  set <tt>true</tt> if assertNoArgs should be skipped
     * @return the found method or <tt>null</tt> if no method was found
     */
    public static Method getOptionalAnnotationMethod(Class<?> classType, Class<? extends Annotation> annotation,
                                                     AnnotationFilter filter, Class returnType, boolean skipArgsCheck) {
        List<Method> methods = findMethod(classType, annotation, filter);
        assertAtMostOne(methods, classType, annotation);
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
     * <p/>
     * Returns more than one method only if they are declared in the same class.
     * As soon as at least one method has been found, no super class will be searched.
     * So a child class will always overwrite the annotated methods from its superclass.
     *
     * @param annotation Type of the annotation to search for
     * @return List of found methods with this annotation
     */
    private static List<Method> findMethod(Class<?> classType, Class<? extends Annotation> annotation) {
        return findMethod(classType, annotation, new AlwaysFilter());
    }

    /**
     * Returns a list of annotated methods in a class hierarchy.
     * <p/>
     * Returns more than one method only if they are declared in the same class.
     * As soon as at least one method has been found, no super class will be searched.
     * So a child class will always overwrite the annotated methods from its superclass.
     *
     * @param annotation Type of the annotation to search for
     * @param filter     Filter to filter search result by annotation values
     * @return List of found methods with this annotation
     */
    private static List<Method> findMethod(Class<?> classType, Class<? extends Annotation> annotation, AnnotationFilter filter) {
        List<Method> methods = new LinkedList<Method>();

        // search in base class
        findMethod(classType, annotation, filter, methods);

        Class<?> searchClass = classType;
        while (methods.size() == 0 && searchClass.getSuperclass() != null) {
            // search in super class
            searchClass = searchClass.getSuperclass();
            findMethod(searchClass, annotation, filter, methods);
        }

        return methods;
    }

    @SuppressWarnings("unchecked")
    private static void findMethod(Class<?> searchClass, Class<? extends Annotation> annotation, AnnotationFilter filter,
                                   List<Method> methods) {
        for (Method method : searchClass.getDeclaredMethods()) {
            Annotation found = method.getAnnotation(annotation);
            if (found != null && filter.allowed(found)) {
                methods.add(method);
            }
        }
    }

    private static void assertExactlyOne(List<Method> methods, Class<?> classType, Class<? extends Annotation> annotation) {
        if (methods.size() == 0) {
            throw new IllegalTestException(
                    format("No method on class %s with annotated %s found", classType.getName(), annotation.getName()));
        }
        if (methods.size() == 1) {
            return;
        }
        throw new IllegalTestException(
                format("Too many methods on class %s with annotation %s", classType.getName(), annotation.getName()));
    }

    private static void assertAtMostOne(List<Method> methods, Class<?> classType, Class<? extends Annotation> annotation) {
        if (methods.size() > 1) {
            throw new IllegalTestException(
                    format("Too many methods on class %s with annotation %s", classType.getName(), annotation.getName()));
        }
    }

    private static void assertNotStatic(Method method) {
        if (Modifier.isStatic(method.getModifiers())) {
            throw new IllegalTestException(format("Method  %s can't be static", method.getName()));
        }
    }

    private static void assertVoidReturnType(Class<?> classType, Method method) {
        assertReturnType(classType, method, Void.TYPE);
    }

    private static void assertReturnType(Class<?> classType, Method method, Class returnType) {
        if (returnType.equals(method.getReturnType())) {
            return;
        }

        throw new IllegalTestException("Method " + classType + "." + method + " should have returnType: " + returnType);
    }

    private static void assertNoArgs(Method method) {
        if (method.getParameterTypes().length == 0) {
            return;
        }

        throw new IllegalTestException(format("Method '%s' can't have any args", method));
    }
}
