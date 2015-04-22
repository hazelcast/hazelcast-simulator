package com.hazelcast.simulator.utils;

import joptsimple.BuiltinHelpFormatter;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.OutputStream;

public final class CliUtils {

    private CliUtils() {
    }

    public static OptionSet initOptionsWithHelp(OptionParser parser, String[] args) {
        try {
            OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

            OptionSet options = parser.parse(args);

            printHelpAndExit(parser, options, helpSpec, System.out);

            return options;
        } catch (OptionException e) {
            throw new CommandLineExitException(e.getMessage() + ". Use --help to get overview of the help options.");
        }
    }

    static void printHelpAndExit(OptionParser parser, OptionSet options, OptionSpec helpSpec, OutputStream sink) {
        if (options.has(helpSpec)) {
            try {
                parser.formatHelpWith(new BuiltinHelpFormatter(160, 2));
                parser.printHelpOn(sink);
            } catch (Exception e) {
                throw new CommandLineExitException("Could not print command line help", e);
            }
            System.exit(0);
        }
    }
}
