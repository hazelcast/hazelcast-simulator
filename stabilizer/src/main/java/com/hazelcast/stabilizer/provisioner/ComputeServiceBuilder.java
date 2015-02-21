package com.hazelcast.stabilizer.provisioner;

import com.google.inject.AbstractModule;
import com.hazelcast.stabilizer.common.StabilizerProperties;
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

import static com.hazelcast.stabilizer.utils.CommonUtils.exitWithError;
import static com.hazelcast.stabilizer.utils.FileUtils.newFile;
import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

public class ComputeServiceBuilder {

    private final static Logger log = Logger.getLogger(ComputeServiceBuilder.class);

    private final StabilizerProperties props;

    public ComputeServiceBuilder(StabilizerProperties props) {
        if (props == null) {
            throw new NullPointerException("props can't be null");
        }
        this.props = props;
    }

    public ComputeService build() {
        ensurePublicPrivateKeyExist();

        String cloudProvider = props.get("CLOUD_PROVIDER");
        String identity = props.get("CLOUD_IDENTITY");
        String credential = props.get("CLOUD_CREDENTIAL");

        if (log.isDebugEnabled()) {
            log.debug("Using CLOUD_PROVIDER: " + cloudProvider);
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
            exitWithError(log, "Unrecognized cloud-provider [" + cloudProvider + "]");
            return null;
        }
    }

    private List<AbstractModule> getModules() {
        return asList(new SLF4JLoggingModule(), new SshjSshClientModule());
    }

    private void ensurePublicPrivateKeyExist() {
        File publicKey = newFile("~", ".ssh", "id_rsa.pub");
        if (!publicKey.exists()) {
            exitWithError(log, "Could not found public key: " + publicKey.getAbsolutePath() + "\n" +
                    "To create a public/private execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
        }

        File privateKey = newFile("~", ".ssh", "id_rsa");
        if (!privateKey.exists()) {
            exitWithError(log, "Public key " + publicKey.getAbsolutePath() + " was found," +
                    " but private key: " + privateKey.getAbsolutePath() + " is missing\n" +
                    "To create a public/private key execute [ssh-keygen -t rsa -C \"your_email@example.com\"]");
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
