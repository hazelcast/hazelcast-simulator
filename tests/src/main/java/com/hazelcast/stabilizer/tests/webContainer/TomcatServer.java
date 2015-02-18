package com.hazelcast.stabilizer.tests.webContainer;

import org.apache.catalina.Context;
import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.Tomcat;

import java.io.File;
import java.util.Random;

public class TomcatServer implements ServletContainer {

    Tomcat tomcat;
    String serverXml;
    String sourceDir;
    int port;
    volatile boolean running;

    public TomcatServer(int port, String sourceDir, String serverXml) throws Exception {
        this.port = port;
        this.sourceDir = sourceDir;
        this.serverXml = serverXml;
        buildTomcat();
    }

    @Override
    public void stop() throws Exception {
        tomcat.stop();
        tomcat.destroy();
        running = false;
    }

    @Override
    public void start() throws Exception {
        buildTomcat();
        running = true;
    }

    @Override
    public void restart() throws Exception {
        stop();
        Thread.sleep(5000);
        start();
    }

    private void buildTomcat() throws LifecycleException {
        tomcat = new Tomcat();
        tomcat.setPort(port);

        String base = System.getProperty("user.home");
        tomcat.setBaseDir(base);


        Context context = tomcat.addContext("/", sourceDir);
        context.getServletContext().setAttribute(Globals.ALT_DD_ATTR, base+"/webapps/"+sourceDir+"/WEB-INF/"+serverXml);
        ContextConfig contextConfig = new ContextConfig();
        context.addLifecycleListener(contextConfig);
        context.setCookies(true);
        context.setBackgroundProcessorDelay(1);
        context.setReloadable(true);

        tomcat.start();
        running = true;
    }

    @Override
    public boolean isRunning() {
        return running;
    }
}
