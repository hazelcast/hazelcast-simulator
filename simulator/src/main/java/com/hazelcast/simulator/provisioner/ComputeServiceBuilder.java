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

import static com.hazelcast.simulator.utils.FileUtils.newFile;
import static com.hazelcast.simulator.utils.FormatUtils.NEW_LINE;
import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

class ComputeServiceBuilder {

    private static final Logger LOGGER = Logger.getLogger(ComputeServiceBuilder.class);

    private final SimulatorProperties props;

    ComputeServiceBuilder(SimulatorProperties props) {
        if (props == null) {
            throw new NullPointerException("props can't be null");
        }
        this.props = props;
    }

    ComputeService build() {
        ensurePublicPrivateKeyExist();

        String cloudProvider = props.get("CLOUD_PROVIDER");
        String identity = props.get("CLOUD_IDENTITY");
        String credential = props.get("CLOUD_CREDENTIAL");

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

    private ContextBuilder newContextBuilder(String cloudProvider) {
        try {
            return ContextBuilder.newBuilder(cloudProvider);
        } catch (NoSuchElementException e) {
            throw new CommandLineExitException("Unrecognized cloud-provider [" + cloudProvider + ']');
        }
    }

    private List<AbstractModule> getModules() {
        return asList(new SLF4JLoggingModule(), new SshjSshClientModule());
    }

    private void ensurePublicPrivateKeyExist() {
        File publicKey = newFile("~", ".ssh", "id_rsa.pub");
        if (!publicKey.exists()) {
            throw new CommandLineExitException("Could not found public key: " + publicKey.getAbsolutePath() + NEW_LINE
                    + "To create a public/private execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }

        File privateKey = newFile("~", ".ssh", "id_rsa");
        if (!privateKey.exists()) {
            throw new CommandLineExitException("Public key " + publicKey.getAbsolutePath() + " was found, "
                    + "but private key: " + privateKey.getAbsolutePath() + " is missing" + NEW_LINE
                    + "To create a public/private key execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }
    }

    private Properties newOverrideProperties() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties properties = new Properties();
        properties.setProperty(POLL_INITIAL_PERIOD, props.get("CLOUD_POLL_INITIAL_PERIOD", "50"));
        properties.setProperty(POLL_MAX_PERIOD, props.get("CLOUD_POLL_MAX_PERIOD", "1000"));
        return properties;
    }
}
