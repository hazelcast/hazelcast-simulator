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
package com.hazelcast.simulator.tests.webContainer;

import org.apache.catalina.Context;
import org.apache.catalina.Globals;
import org.apache.catalina.LifecycleException;
import org.apache.catalina.startup.ContextConfig;
import org.apache.catalina.startup.Tomcat;

import static com.hazelcast.simulator.utils.FileUtils.USER_HOME;

public class TomcatContainer implements ServletContainer {

    Tomcat tomcat;
    String serverXml;
    String sourceDir;
    int port;
    volatile boolean running;

    public TomcatContainer(int port, String sourceDir, String serverXml) throws Exception {
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

        String base = USER_HOME;
        tomcat.setBaseDir(base);

        Context context = tomcat.addContext("/", sourceDir);
        context.getServletContext().setAttribute(Globals.ALT_DD_ATTR, base + "/webapps/" + sourceDir + "/WEB-INF/" + serverXml);
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
