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
package com.hazelcast.simulator.tests.quorum;

import com.hazelcast.core.IQueue;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.simulator.test.AbstractTest;
import com.hazelcast.simulator.test.BaseThreadState;
import com.hazelcast.simulator.test.annotations.Setup;
import com.hazelcast.simulator.test.annotations.TimeStep;

public class QuorumQueueTest extends AbstractTest {

	// properties
	private IQueue<Long> queue;
	public int keyCount = 1000;

	@Setup
	@SuppressWarnings("unchecked")
	public void setup() {
		queue = targetInstance.getQueue(name);
	}

	@TimeStep
	public void put(BaseThreadState state) {
		try {
			queue.put(0L);
			queue.poll();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (QuorumException qe){
			qe.printStackTrace();
		}
		
	}
}
