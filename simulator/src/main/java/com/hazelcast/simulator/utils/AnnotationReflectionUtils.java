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

import com.hazelcast.simulator.test.annotations.InjectProbe;

import java.lang.reflect.Field;

public final class AnnotationReflectionUtils {

    static final AnnotationFilter.AlwaysFilter ALWAYS_FILTER = new AnnotationFilter.AlwaysFilter();

    private AnnotationReflectionUtils() {
    }

    public static String getProbeName(Field field) {
        if (field == null) {
            return null;
        }

        InjectProbe injectProbe = field.getAnnotation(InjectProbe.class);
        if (injectProbe != null && !InjectProbe.NULL.equals(injectProbe.name())) {
            return injectProbe.name();
        }
        return field.getName();
    }

    public static boolean isPartOfTotalThroughput(Field field) {
        if (field == null) {
            return false;
        }

        InjectProbe injectProbe = field.getAnnotation(InjectProbe.class);
        if (injectProbe != null) {
            return injectProbe.useForThroughput();
        }
        return false;
    }
}
