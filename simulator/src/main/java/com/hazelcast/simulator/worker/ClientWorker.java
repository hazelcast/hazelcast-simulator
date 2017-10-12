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
package com.hazelcast.simulator.worker;

/**
 * This class serves no other purpose than to provide a name in the jps listing that reflects that the JVM is a Client Worker.
 * It has no other purpose and it will delegate all its work to the {@link MemberWorker} class.
 */
public final class ClientWorker {

    private ClientWorker() {
    }

    public static void main(String[] args) {
        MemberWorker.main(args);
    }
}
