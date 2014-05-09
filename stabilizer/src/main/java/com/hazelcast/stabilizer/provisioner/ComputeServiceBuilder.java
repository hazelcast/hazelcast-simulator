package com.hazelcast.stabilizer.provisioner;

import com.hazelcast.stabilizer.Utils;
import com.hazelcast.stabilizer.common.StabilizerProperties;
import org.jclouds.ContextBuilder;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import java.io.File;
import java.util.Properties;

import static java.util.Arrays.asList;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_INITIAL_PERIOD;
import static org.jclouds.compute.config.ComputeServiceProperties.POLL_MAX_PERIOD;

public class ComputeServiceBuilder {

    private final StabilizerProperties props;

    public ComputeServiceBuilder(StabilizerProperties props){
        this.props = props;
    }

    public ComputeService build() {
        //http://javadocs.jclouds.cloudbees.net/org/jclouds/compute/config/ComputeServiceProperties.html
        Properties overrides = new Properties();
        overrides.setProperty(POLL_INITIAL_PERIOD, props.get("CLOUD_POLL_INITIAL_PERIOD", "50"));
        overrides.setProperty(POLL_MAX_PERIOD, props.get("CLOUD_POLL_MAX_PERIOD", "1000"));

        String credentials = props.get("CLOUD_CREDENTIAL");
        File file = new File(credentials);
        if (file.exists()) {
            credentials = Utils.fileAsText(file);
        }

        return ContextBuilder.newBuilder(props.get("CLOUD_PROVIDER"))
                .overrides(overrides)
                .credentials(props.get("CLOUD_IDENTITY"), credentials)
                .modules(asList(new SLF4JLoggingModule(), new SshjSshClientModule()))
                .buildView(ComputeServiceContext.class)
                .getComputeService();
    }
}
