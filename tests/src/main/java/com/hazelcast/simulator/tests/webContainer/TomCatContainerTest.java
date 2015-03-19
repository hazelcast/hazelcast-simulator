package com.hazelcast.simulator.tests.webContainer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.simulator.test.TestContext;
import com.hazelcast.simulator.test.TestRunner;
import com.hazelcast.simulator.test.annotations.Run;
import com.hazelcast.simulator.test.annotations.Setup;

import static com.hazelcast.simulator.utils.CommonUtils.sleepSeconds;
import static org.junit.Assert.assertEquals;


/*
* This "test" simple starts a Tomcat server instance,  which is in turn configured vai an xml file to, create an hazelcast
* instance.  this "test" only sleeps in it's run method.  when running this sets,  we should consider,  who should take
* responsibility for creating hazelcast instances
* */
public class TomCatContainerTest {

    private final static ILogger log = Logger.getLogger(TomCatContainerTest.class);
    public String basename = this.getClass().getSimpleName();
    public int port=6555;
    public String webAppPath="webapp";
    public String configXml="tomcat-hz.xml";

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

    public static void main(String[] args) throws Throwable {
        TomCatContainerTest test = new TomCatContainerTest();
        new TestRunner<TomCatContainerTest>(test).run();
    }
}