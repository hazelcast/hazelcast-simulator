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

 package com.hazelcast.simulator.tests.map;

 import com.hazelcast.collection.IList;
 import com.hazelcast.collection.ISet;
 import com.hazelcast.core.DistributedObject;
 import com.hazelcast.map.IMap;
 import com.hazelcast.multimap.MultiMap;
 import com.hazelcast.simulator.hz.HazelcastTest;
 import com.hazelcast.simulator.test.BaseThreadState;
 import com.hazelcast.simulator.test.annotations.Setup;
 import com.hazelcast.simulator.test.annotations.TimeStep;

 import java.util.Arrays;
 import java.util.Map;
 import java.util.UUID;
 import java.util.concurrent.ConcurrentHashMap;

 public class MetricsCarouselTest extends HazelcastTest {

     private final Map<String, ObjectInfo> createdObjects = new ConcurrentHashMap<>();
     public int keyDomain = 10000;
     public int valueCount = 10000;

    @Setup
    public void setUp() {
        targetInstance.getDistributedObjects()
                .forEach(it -> it.destroy());
    }

     @TimeStep(prob = 1)
     // The scenario is designed mainly to check the unique metric IDs overflow
     // https://hazelcast.atlassian.net/browse/MC-3012
     // There is a use case where customers create a lot of unique short living objects
     // like data structures and/or clients. These objects live long enough to be populated to the MC
     // in MC there's an internal in memory storage which stored these objects' IDs and the ID's weren't
     // persisted together with the metrics themselves in a timely manner. This with a time may led to OOME

     //The test here doesn't really rely on super high performance
     // but rather creates the objects as unique as possible
     public void createMapsAndDeleteThemLater(BaseThreadState state) throws InterruptedException {
         cleanupOldObjects();
         String name = UUID.randomUUID() + UUID.randomUUID().toString();
         int randomIntOfTheDay = state.randomInt();

         IMap<Integer, Integer> map = targetInstance.getMap("map-%s".formatted(name));
         map.put(randomIntOfTheDay, randomIntOfTheDay);

         MultiMap<Integer, Integer> multiMap = targetInstance.getMultiMap("multimap-%s".formatted(name));
         multiMap.put(randomIntOfTheDay, randomIntOfTheDay);

         IList<Integer> list = targetInstance.getList("list-%s".formatted(name));
         list.add(randomIntOfTheDay);

         ISet<Integer> set = targetInstance.getSet("set-%s".formatted(name));
         set.add(randomIntOfTheDay);

         ObjectInfo objectInfo = new ObjectInfo(System.currentTimeMillis(), map, multiMap, list, set);
         createdObjects.put(name, objectInfo);
         Thread.sleep(5_000);
     }

     private static class ObjectInfo {
         long creationTime;
         DistributedObject[] dataStructures;

         ObjectInfo(long creationTime, DistributedObject... dataStructures) {
             this.creationTime = creationTime;
             this.dataStructures = dataStructures;
         }
     }

     private void cleanupOldObjects() {
         long currentTime = System.currentTimeMillis();
         synchronized (createdObjects) {
             createdObjects.entrySet().removeIf(entry -> {
                 ObjectInfo info = entry.getValue();
                 if ((currentTime - info.creationTime) > 30_000) {
                     Arrays.stream(info.dataStructures).forEach(DistributedObject::destroy);
                     return true;
                 }
                 return false;
             });
         }
     }
 }
