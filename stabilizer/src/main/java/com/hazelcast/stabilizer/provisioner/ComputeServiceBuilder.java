package com.hazelcast.stabilizer.provisioner;

import com.google.inject.AbstractModule;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.stabilizer.common.StabilizerProperties;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.util.List;
import java.util.Properties;

import static com.hazelcast.stabilizer.Utils.fileAsText;
import static com.hazelcast.stabilizer.Utils.newFile;
import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

public class ComputeServiceBuilder {

    private final static ILogger log = Logger.getLogger(ComputeServiceBuilder.class);

    private final StabilizerProperties props;

    public ComputeServiceBuilder(StabilizerProperties props) {
        if (props == null) {
            throw new NullPointerException("props can't be null");
        }
        this.props = props;
    }

    public ComputeService build() {
        String cloudProvider = props.get("CLOUD_PROVIDER");

        String identity = load("CLOUD_IDENTITY");
        String credential = load("CLOUD_CREDENTIAL");

        if (log.isFinestEnabled()) {
            log.finest("Using CLOUD_PROVIDER: " + cloudProvider);
        }

        return ContextBuilder.newBuilder(cloudProvider)
                .overrides(newOverrideProperties())
                .credentials(identity, credential)
                .modules(getModules())
                .buildView(ComputeServiceContext.class)
                .
                .getComputeService();
    }

    private List<AbstractModule> getModules() {
        return asList(new SLF4JLoggingModule(), new SshjSshClientModule());
    }

    private Properties newOverrideProperties() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties properties = new Properties();
        properties.setProperty(POLL_INITIAL_PERIOD, props.get("CLOUD_POLL_INITIAL_PERIOD", "50"));
        properties.setProperty(POLL_MAX_PERIOD, props.get("CLOUD_POLL_MAX_PERIOD", "1000"));
        return properties;
    }

    private String load(String property) {
        String value = props.get(property,"");

        File file = newFile(value);
        if (file.exists()) {
            if (log.isFinestEnabled()) {
                log.finest("Loading " + property + " from file: " + file.getAbsolutePath());
            }
            value = fileAsText(file);
        }
        return value;
    }
}
