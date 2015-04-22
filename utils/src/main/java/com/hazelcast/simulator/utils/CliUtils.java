package com.hazelcast.simulator.utils;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

public final class CliUtils {

    private CliUtils() {
    }

    public static OptionSet initOptionsWithHelp(OptionParser parser, String[] args) {
        try {
            OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();
            OptionSet options = parser.parse(args);

            printHelpAndExit(parser, options, helpSpec);

            return options;
        } catch (OptionException e) {
            throw new CommandLineExitException(e.getMessage() + ". Use --help to get overview of the help options.");
        }
    }

    private static void printHelpAndExit(OptionParser parser, OptionSet options, OptionSpec helpSpec) {
        if (options.has(helpSpec)) {
            try {
                parser.printHelpOn(System.out);
            } catch (Exception e) {
                throw new CommandLineExitException("Could not print command line help", e);
            }
            System.exit(0);
        }
    }
}
