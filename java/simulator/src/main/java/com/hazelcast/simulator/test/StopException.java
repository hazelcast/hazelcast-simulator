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
package com.hazelcast.simulator.test;

/**
 * The {@link StopException} can be thrown in an {@link com.hazelcast.simulator.test.annotations.TimeStep} based test
 * to signal that the timestep thread doesn't want to run any longer, but doesn't want to cause any failures. Also it doesn't
 * impact any other timestep-thread.
 *
 * e.g.
 *
 * <pre>
 * {@literal @}TimeStep
 * public void timestep(ThreadState state){
 *     if(state.iterations == 1000) throw new StopException();
 *     state.iterations++;
 * }
 * </pre>
 *
 * The StopException can only be thrown in the TimeStep exceptions without leading to failure. If a StopException is thrown
 * in any other method, it will be considered a normal exception.
 */
public class StopException extends RuntimeException {
}
