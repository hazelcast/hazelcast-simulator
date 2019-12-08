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
package com.hazelcast.simulator.tests.concurrent.lock;

import java.io.Serializable;

public class LockCounter implements Serializable {

   public long locked;
   public long increased;
   public long unlocked;

   public LockCounter() {
   }

   public void add(LockCounter counter) {
      locked += counter.locked;
      increased += counter.increased;
      unlocked += counter.unlocked;
   }

   @Override
   public String toString() {
      return "LockCounter{"
            + "locked=" + locked
            + ", inced=" + increased
            + ", unlocked=" + unlocked
            + '}';
   }
}