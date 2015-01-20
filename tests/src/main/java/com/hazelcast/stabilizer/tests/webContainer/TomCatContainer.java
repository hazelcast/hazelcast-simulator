package com.hazelcast.stabilizer.tests.webContainer;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.test.TestContext;
import com.hazelcast.stabilizer.test.TestRunner;
import com.hazelcast.stabilizer.test.annotations.Run;
import com.hazelcast.stabilizer.test.annotations.Setup;
import com.hazelcast.stabilizer.test.annotations.Verify;
import com.hazelcast.stabilizer.test.annotations.Warmup;

import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.SimpleFormatter;

import static com.hazelcast.stabilizer.test.utils.TestUtils.sleepMs;
import static org.junit.Assert.assertEquals;


/*
* This "test" simple starts a Tomcat server instance,  which is in turn configured vai an xml file to, create an hazelcast
* instance.  this "test" only sleeps in it's run method.  when running this sets,  we should consider,  who should take
* responsibility for creating hazelcast instances
* */
public class TomCatContainer {

    private final static ILogger log = Logger.getLogger(TomCatContainer.class);
    public String basename = this.getClass().getSimpleName();
    public int port=6555;
    public String webAppPath="webapp";
    public String configXml="tomcat-hz.xml";

    private TomcatServer tomcat;
    private TestContext testContext;

    @Setup
    public void setup(TestContext testContext) throws Exception {

        this.testContext = testContext;
        TomcatServer tomcat = new TomcatServer(port, webAppPath, configXml);
    }

    @Warmup(global = true)
    public void warmup() throws Exception {

    }

    @Run
    public void run() {
        while (!testContext.isStopped()) {
            sleepMs(2000);
        }
    }


    @Verify(global = true)
    public void verify() throws Exception {

    }

    public static void main(String[] args) throws Throwable {
        TomCatContainer test = new TomCatContainer();
        new TestRunner<TomCatContainer>(test).run();
    }
}