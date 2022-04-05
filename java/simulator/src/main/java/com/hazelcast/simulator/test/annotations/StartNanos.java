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
package com.hazelcast.simulator.test.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * An annotation put on the argument of a timestep method to indicate that the argument contains the startime in
 * nanos. It depends on the metronome configuration if it contains the intended starttime or actual start time.
 * If the test isn't using a metronome, this method contains the actual time (System.nanotime).
 *
 * This is required for e.g. async method call testing where the timestep runner can't take care of recording the
 * value on the probe because it doesn't know when the asynchronous method has completed.
 *
 * If you need this annotation, then probably you also need to pass the Probe as an argument. For a full example
 * AsyncAtomicLongTest.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.PARAMETER)
public @interface StartNanos {
}
