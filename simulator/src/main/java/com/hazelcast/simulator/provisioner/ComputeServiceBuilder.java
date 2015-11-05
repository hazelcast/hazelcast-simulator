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
package com.hazelcast.simulator.provisioner;

import com.google.inject.AbstractModule;
import com.hazelcast.simulator.common.SimulatorProperties;
import com.hazelcast.simulator.utils.CommandLineExitException;
import org.apache.log4j.Logger;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Properties;

import static com.hazelcast.simulator.utils.CloudProviderUtils.isStatic;
import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.util.Arrays.asList;
import static org.jclouds.ContextBuilder.newBuilder;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

class ComputeServiceBuilder {

    private static final File PUBLIC_KEY = newFile("~", ".ssh", "id_rsa.pub");
    private static final File PRIVATE_KEY = newFile("~", ".ssh", "id_rsa");

    private static final Logger LOGGER = Logger.getLogger(ComputeServiceBuilder.class);

    private final SimulatorProperties properties;

    ComputeServiceBuilder(SimulatorProperties properties) {
        if (properties == null) {
            throw new NullPointerException("properties can't be null");
        }
        this.properties = properties;
    }

    static void ensurePublicPrivateKeyExist(File publicKey, File privateKey) {
        if (!publicKey.exists()) {
            throw new CommandLineExitException("Public key " + publicKey.getAbsolutePath() + " not found." + NEW_LINE
                    + "To create a public/private execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }
        if (!privateKey.exists()) {
            throw new CommandLineExitException("Public key " + publicKey.getAbsolutePath() + " was found, "
                    + "but private key " + privateKey.getAbsolutePath() + " is missing" + NEW_LINE
                    + "To create a public/private key execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }
    }

    @SuppressWarnings("PMD.PreserveStackTrace")
    static ContextBuilder newContextBuilder(String cloudProvider) {
        try {
            return newBuilder(cloudProvider);
        } catch (NoSuchElementException e) {
            throw new CommandLineExitException("Unrecognized cloud-provider [" + cloudProvider + ']');
        }
    }

    ComputeService build() {
        ensurePublicPrivateKeyExist(PUBLIC_KEY, PRIVATE_KEY);

        String cloudProvider = properties.get("CLOUD_PROVIDER");
        String identity = properties.get("CLOUD_IDENTITY");
        String credential = properties.get("CLOUD_CREDENTIAL");

        if (isStatic(cloudProvider)) {
            return null;
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Using CLOUD_PROVIDER: " + cloudProvider);
        }

        ContextBuilder contextBuilder = newContextBuilder(cloudProvider);
        return contextBuilder.overrides(newOverrideProperties())
                .credentials(identity, credential)
                .modules(getModules())
                .buildView(ComputeServiceContext.class)
                .getComputeService();
    }

    private Properties newOverrideProperties() {
        // http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties newProperties = new Properties();
        newProperties.setProperty(POLL_INITIAL_PERIOD, this.properties.get("CLOUD_POLL_INITIAL_PERIOD", "50"));
        newProperties.setProperty(POLL_MAX_PERIOD, this.properties.get("CLOUD_POLL_MAX_PERIOD", "1000"));
        return newProperties;
    }

    private List<AbstractModule> getModules() {
        return asList(new SLF4JLoggingModule(), new SshjSshClientModule());
    }
}
