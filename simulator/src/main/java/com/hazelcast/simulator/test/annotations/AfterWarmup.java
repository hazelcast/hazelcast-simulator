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
package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The {@link AfterWarmup} annotation can be placed on a method to trigger that it should reset any global state after
 * the warmup has completed.
 *
 * The {@link AfterWarmup} is used for the sake of warmup in combination with {@link TimeStep} based test. When a warmup duration
 * is defined, the timestep methods will be called in exactly the same way as during the actual run phase. Once the warmup
 * period has completed, the runner threads and their thread-state are discarded and new ones will be created for the run period.
 *
 * However it could be that some global state was modified during the warmup phase that needs to be reset. This global state
 * is reset by methods with the {@link AfterWarmup} annotation. This is done between the warmup and the run phase.
 *
 * If no warmup duration is defined, the {@link AfterWarmup} method is not called.
 *
 * Multiple {@link AfterWarmup} methods are allowed. The {@link AfterWarmup} methods on a subclass are executed before
 * the {@link AfterWarmup} methods on a super class, however there is no ordering within the same class. This is the same
 * semantics as provided by junit. Local {@link AfterWarmup} always happens before global {@link AfterWarmup}.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface AfterWarmup {
    boolean global() default true;
}
