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

    public String basename = this.getClass().getSimpleName();
    public int port = 6555;
    public String webAppPath = "webapp";
    public String configXml = "tomcat-hz.xml";

    private TomcatContainer tomcat;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {
        this.testContext = testContext;
        tomcat = new TomcatContainer(port, webAppPath, configXml);
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