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

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.simulator.utils.Preconditions.checkNotNull;
import static java.lang.String.format;

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

    public AnnotatedMethodRetriever withPublicNonStaticModifier() {
        mustBePublic = true;
        mustBeNotStatic = true;
        return this;
    }

    public AnnotatedMethodRetriever withReturnType(Class returnType) {
        this.returnType = returnType;
        return this;
    }

    public AnnotatedMethodRetriever withVoidReturnType() {
        return withReturnType(Void.TYPE);
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
        if (mustBoNoArg) {
            if (method.getParameterTypes().length != 0) {
                throw new ReflectionException(format("Method '%s' can't have any args", method));
            }
        }
    }

    private void verifyReturnType(Method method) {
        if (returnType != null) {
            if (!returnType.isAssignableFrom(method.getReturnType())) {
                throw new ReflectionException(format("Method '%s' should have returnType %s", method, returnType));
            }
        }
    }

    private void verifyNotStatic(Method method) {
        if (mustBeNotStatic) {
            if (Modifier.isStatic(method.getModifiers())) {
                throw new ReflectionException(format("Method '%s' should no be static", method));
            }
        }
    }

    private void verifyPublic(Method method) {
        if (mustBePublic) {
            if (!Modifier.isPublic(method.getModifiers())) {
                throw new ReflectionException(format("Method '%s' should be public", method));
            }
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

    private void findDeclaredMethods(Class searchClass, List<Method> methods) {
        for (Method method : searchClass.getDeclaredMethods()) {
            Annotation found = method.getAnnotation(annotationClazz);

            if (found == null) {
                continue;
            }

            if (filter != null && !filter.allowed(found)) {
                continue;
            }

            methods.add(method);
        }
    }
}
