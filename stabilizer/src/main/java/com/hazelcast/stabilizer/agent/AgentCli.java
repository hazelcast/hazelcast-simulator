package com.hazelcast.stabilizer.agent;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;
import java.io.IOException;

import static com.hazelcast.stabilizer.Utils.getFile;

public class AgentCli {

    private final OptionParser parser = new OptionParser();
    private final OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
    private final OptionSpec<String> javaInstallationsFileSpec = parser.accepts("javaInstallationsFile",
            "A property file containing the Java installations used by Workers launched by this Agent")
            .withRequiredArg().ofType(String.class)
            .defaultsTo(Agent.STABILIZER_HOME + File.separator + "conf" + File.separator + "java-installations.properties");

    public static void init(Agent agent, String[] args) throws IOException {
        AgentCli agentOptionSpec = new AgentCli();
        OptionSet options = agentOptionSpec.parser.parse(args);

        if (options.has(agentOptionSpec.helpSpec)) {
            agentOptionSpec.parser.printHelpOn(System.out);
            System.exit(0);
        }

        agent.javaInstallationsFile = getFile(
                agentOptionSpec.javaInstallationsFileSpec,
                options,
                "Java Installations config file");
    }
}
