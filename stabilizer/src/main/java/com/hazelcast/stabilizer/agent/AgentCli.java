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

    public static void init(Agent agent, String[] args) throws IOException {
        AgentCli agentOptionSpec = new AgentCli();
        OptionSet options = agentOptionSpec.parser.parse(args);

        if (options.has(agentOptionSpec.helpSpec)) {
            agentOptionSpec.parser.printHelpOn(System.out);
            System.exit(0);
        }

    }
}
