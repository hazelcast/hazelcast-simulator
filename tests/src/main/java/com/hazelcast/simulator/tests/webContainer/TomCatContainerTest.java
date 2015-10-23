/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.simulator.tests.webContainer;

import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;

/*
* This "test" simple starts a Tomcat server instance,  which is in turn configured vai an xml file to, create an hazelcast
* instance.  this "test" only sleeps in it's run method.  when running this sets,  we should consider,  who should take
* responsibility for creating hazelcast instances
* */
public class TomCatContainerTest {

    public String basename = TomCatContainerTest.class.getSimpleName();
    public int port = 6555;
    public String webAppPath = "webapp";
    public String configXml = "tomcat-hz.xml";

    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        new TomcatContainer(port, webAppPath, configXml);
    }

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            sleepSeconds(2);
        }
    }

    public static void main(String[] args) throws Exception {
        TomCatContainerTest test = new TomCatContainerTest();
        new TestRunner<TomCatContainerTest>(test).run();
    }
}
