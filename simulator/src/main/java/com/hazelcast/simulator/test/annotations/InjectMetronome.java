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

import com.hazelcast.simulator.worker.metronome.MetronomeType;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import static com.hazelcast.simulator.worker.metronome.MetronomeType.NOP;

/**
 * Annotates {@link com.hazelcast.simulator.worker.metronome.Metronome} fields.
 *
 * @deprecated will be removed in 0.10.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface InjectMetronome {

    /**
     * Defines the {@link com.hazelcast.simulator.worker.metronome.Metronome} interval in milliseconds.
     *
     * @return the {@link com.hazelcast.simulator.worker.metronome.Metronome} interval in milliseconds
     */
    @Deprecated
    int intervalMillis() default 0;

    /**
     * Defines the {@link MetronomeType}.
     *
     * @return the {@link MetronomeType}
     */
    @Deprecated
    MetronomeType type() default NOP;
}
