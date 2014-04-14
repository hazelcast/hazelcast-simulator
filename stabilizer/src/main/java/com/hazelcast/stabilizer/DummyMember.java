package com.hazelcast.stabilizer;

import com.hazelcast.config.Config;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

import java.io.File;

import static com.hazelcast.stabilizer.Utils.exitWithError;
import static com.hazelcast.stabilizer.Utils.getStablizerHome;
import static com.hazelcast.stabilizer.Utils.getVersion;
import static java.lang.String.format;

public class DummyMember {
    private final static File STABILIZER_HOME = getStablizerHome();
    private final static ILogger log = Logger.getLogger(DummyMember.class);
    private File hzFile;

    private void run() throws Exception {
        Config config = new XmlConfigBuilder(hzFile.getAbsolutePath()).build();
        HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(config);

        for (DistributedObject distributedObject : hazelcastInstance.getDistributedObjects()) {
            System.out.println(distributedObject.getClass() + " " + distributedObject.getName());
        }
    }

    public static void main(String[] args) throws Exception {
        log.info("Hazelcast Heart Attack Dummy Member");
        log.info(format("Version: %s", getVersion()));
        log.info(format("STABILIZER_HOME: %s", STABILIZER_HOME));

        OptionParser parser = new OptionParser();
        OptionSpec<String> hzFileSpec = parser.accepts("hzFile", "The Hazelcast xml configuration file")
                .withRequiredArg().ofType(String.class).defaultsTo(STABILIZER_HOME + File.separator + "conf" + File.separator + "trainee-hazelcast.xml");

        OptionSpec helpSpec = parser.accepts("help", "Show help").forHelp();

        OptionSet options;
        DummyMember member = new DummyMember();

        try {
            options = parser.parse(args);

            if (options.has(helpSpec)) {
                parser.printHelpOn(System.out);
                System.exit(0);
            }

            File hzFile = new File(options.valueOf(hzFileSpec));
            if (!hzFile.exists()) {
                exitWithError(format("Hazelcast config file [%s] does not exist.\n", hzFile));
            }
            member.hzFile = hzFile;
        } catch (OptionException e) {
            Utils.exitWithError(e.getMessage() + ". Use --help to get overview of the help options.");
        }

        try {
            member.run();
            System.exit(0);
        } catch (Exception e) {
            log.severe("Failed to run workout", e);
            System.exit(1);
        }
    }

}
