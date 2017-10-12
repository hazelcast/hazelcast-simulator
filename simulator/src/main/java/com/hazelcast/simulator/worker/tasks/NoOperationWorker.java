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
package com.hazelcast.simulator.worker.tasks;

/**
 * Worker implementation which does nothing.
 *
 * Can be used on nodes which should not interact with the cluster.
 *
 * @deprecated is likely to be removed in Simulator 0.10.
 */
public final class NoOperationWorker extends VeryAbstractWorker {

    @Override
    public void run() throws Exception {
        stopTestContext();
    }
}
