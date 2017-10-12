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
package com.hazelcast.simulator.protocol.operation;

/**
 * Marker interface for all Simulator operations, which are the serialized payload of a
 * {@link com.hazelcast.simulator.protocol.core.SimulatorMessage}.
 *
 * Is processed by {@link OperationCodec} with a given {@link com.hazelcast.simulator.protocol.processors.OperationProcessor}.
 */
public interface SimulatorOperation {
}
