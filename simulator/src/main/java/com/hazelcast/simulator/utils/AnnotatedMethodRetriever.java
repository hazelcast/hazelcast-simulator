/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.lang.reflect.Modifier.isPublic;
import static java.lang.reflect.Modifier.isStatic;

public class AnnotatedMethodRetriever {

    private final Class clazz;
    private final Class annotationClazz;

    private Class returnType;
    private boolean mustBePublic;
    private boolean mustBeNotStatic;
    private boolean mustBoNoArg;
    private AnnotationFilter filter;

    public AnnotatedMethodRetriever(Class clazz, Class<? extends Annotation> annotationClazz) {
        this.clazz = checkNotNull(clazz, "clazz can't be null");
        this.annotationClazz = checkNotNull(annotationClazz, "annotationClazz can't be null");
    }

    public AnnotatedMethodRetriever withVoidReturnType() {
        return withReturnType(Void.TYPE);
    }

    public AnnotatedMethodRetriever withReturnType(Class returnType) {
        this.returnType = returnType;
        return this;
    }

    public AnnotatedMethodRetriever withPublicNonStaticModifier() {
        mustBePublic = true;
        mustBeNotStatic = true;
        return this;
    }

    public AnnotatedMethodRetriever withoutArgs() {
        mustBoNoArg = true;
        return this;
    }

    public AnnotatedMethodRetriever withFilter(AnnotationFilter filter) {
        this.filter = filter;
        return this;
    }

    public Method find() {
        List<Method> methods = findAll();
        switch (methods.size()) {
            case 0:
                return null;
            case 1:
                return methods.iterator().next();
            default:
                throw new ReflectionException(format("Too many methods on class %s with annotation %s", clazz.getName(),
                        annotationClazz.getName()));
        }
    }

    public List<Method> findAll() {
        List<Method> methods = findAllDeclaredMethods(clazz);
        for (Method method : methods) {
            verifyPublic(method);
            verifyNotStatic(method);
            verifyReturnType(method);
            verifyArgs(method);
            method.setAccessible(true);
        }
        return methods;
    }

    private void verifyArgs(Method method) {
        if (mustBoNoArg && method.getParameterTypes().length != 0) {
            throw new ReflectionException(format("Method '%s' can't have any args", method));
        }
    }

    @SuppressWarnings("unchecked")
    private void verifyReturnType(Method method) {
        if (returnType != null && !returnType.isAssignableFrom(method.getReturnType())) {
            throw new ReflectionException(format("Method '%s' should have returnType %s", method, returnType));
        }
    }

    private void verifyNotStatic(Method method) {
        if (mustBeNotStatic && isStatic(method.getModifiers())) {
            throw new ReflectionException(format("Method '%s' should no be static", method));
        }
    }

    private void verifyPublic(Method method) {
        if (mustBePublic && !isPublic(method.getModifiers())) {
            throw new ReflectionException(format("Method '%s' should be public", method));
        }
    }

    private List<Method> findAllDeclaredMethods(Class classType) {
        List<Method> methods = new ArrayList<Method>();
        do {
            findDeclaredMethods(classType, methods);
            if (!methods.isEmpty()) {
                break;
            }

            classType = classType.getSuperclass();
        } while (classType != null);

        return methods;
    }

    @SuppressWarnings("unchecked")
    private void findDeclaredMethods(Class searchClass, List<Method> methods) {
        for (Method method : searchClass.getDeclaredMethods()) {
            Annotation found = method.getAnnotation(annotationClazz);
            if (found == null) {
                continue;
            }

            if (filter != null && !filter.allowed(found)) {
                continue;
            }

            if (!isOverridden(methods, method)) {
                methods.add(method);
            }
        }
    }

    private static boolean isOverridden(List<Method> subMethods, Method superMethod) {
        for (Method subMethod : subMethods) {
            if (!subMethod.getName().equals(superMethod.getName())) {
                continue;
            }

            Class<?>[] subParamTypes = subMethod.getParameterTypes();
            Class<?>[] methodParamTypes = superMethod.getParameterTypes();
            if (subParamTypes.length != methodParamTypes.length) {
                continue;
            }

            boolean equalParameters = true;
            for (int i = 0; i < subParamTypes.length; i++) {
                if (!subParamTypes[i].equals(methodParamTypes[i])) {
                    equalParameters = false;
                    break;
                }
            }

            if (!equalParameters) {
                continue;
            }

            if (!superMethod.getReturnType().equals(subMethod.getReturnType())) {
                continue;
            }

            // TODO: in the future we need to deal with covariant return types, bridge methods and parameter types in the methods
            return true;
        }
        return false;
    }
}
