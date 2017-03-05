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

import com.hazelcast.simulator.test.annotations.Prepare;
import com.hazelcast.simulator.test.annotations.Teardown;
import com.hazelcast.simulator.test.annotations.Verify;

import java.lang.annotation.Annotation;

/**
 * This class filters Annotations, e.g. after their values.
 *
 * @param <A> Class of type Annotation
 */
public interface AnnotationFilter<A extends Annotation> {

    boolean allowed(A annotation);

    class TeardownFilter implements AnnotationFilter<Teardown> {
        private final boolean isGlobal;

        public TeardownFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Teardown teardown) {
            return teardown.global() == isGlobal;
        }
    }

    class PrepareFilter implements AnnotationFilter<Prepare> {
        private final boolean isGlobal;

        public PrepareFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Prepare verify) {
            return verify.global() == isGlobal;
        }
    }

    class VerifyFilter implements AnnotationFilter<Verify> {
        private final boolean isGlobal;

        public VerifyFilter(boolean isGlobal) {
            this.isGlobal = isGlobal;
        }

        @Override
        public boolean allowed(Verify verify) {
            return verify.global() == isGlobal;
        }
    }
}
