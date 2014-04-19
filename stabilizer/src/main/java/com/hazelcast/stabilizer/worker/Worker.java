/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.stabilizer.worker;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.Utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;

import static com.hazelcast.stabilizer.Utils.writeObject;
import static java.lang.String.format;

public class Worker {

    final static ILogger log = Logger.getLogger(Worker.class.getName());

    public static final String WORKER_EXECUTOR = "Worker:Executor";

    private String workerId;
    private HazelcastInstance hz;
    private String workerHzFile;

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setWorkerHzFile(String workerHzFile) {
        this.workerHzFile = workerHzFile;
    }

    public void start() {
        log.info("Creating Worker HazelcastInstance");
        this.hz = createHazelcastInstance();
        log.info("Successfully created Worker HazelcastInstance");

        signalStartToAgent();
    }

    private void signalStartToAgent() {
        InetSocketAddress address = hz.getCluster().getLocalMember().getInetSocketAddress();
        File file = new File(workerId + ".address");
        writeObject(address, file);
    }

    private HazelcastInstance createHazelcastInstance() {
        XmlConfigBuilder configBuilder;
        try {
            configBuilder = new XmlConfigBuilder(workerHzFile);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

        Config config = configBuilder.build();
        return Hazelcast.newHazelcastInstance(config);
    }

    private static void logInterestingSystemProperties() {
        logSystemProperty("java.class.path");
        logSystemProperty("java.home");
        logSystemProperty("java.vendor");
        logSystemProperty("java.vendor.url");
        logSystemProperty("java.version");
        logSystemProperty("os.arch");
        logSystemProperty("os.name");
        logSystemProperty("os.version");
        logSystemProperty("user.dir");
        logSystemProperty("user.home");
        logSystemProperty("user.name");
        logSystemProperty("STABILIZER_HOME");
        logSystemProperty("hazelcast.logging.type");
        logSystemProperty("workerId");
        logSystemProperty("log4j.configuration");
    }

    private static void logSystemProperty(String name) {
        log.info(format("%s=%s", name, System.getProperty(name)));
    }

    public static void main(String[] args) {
        log.info("Starting Stabilizer Worker");
        logInterestingSystemProperties();

        String workerId = args[0];
        log.info("Worker id:" + workerId);
        String workerHzFile = args[1];
        log.info("Worker hz config file:" + workerHzFile);
        log.info(Utils.asText(new File(workerHzFile)));

        System.setProperty("workerId", workerId);

        Worker worker = new Worker();
        worker.setWorkerId(workerId);
        worker.setWorkerHzFile(workerHzFile);
        worker.start();

        log.info("Successfully started Hazelcast Stabilizer Worker:" + workerId);
    }
}
