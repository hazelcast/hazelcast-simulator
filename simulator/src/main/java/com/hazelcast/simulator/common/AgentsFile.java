package com.hazelcast.simulator.common;

import org.apache.log4j.Logger;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import static com.hazelcast.simulator.utils.FileUtils.fileAsText;
import static com.hazelcast.simulator.utils.FileUtils.writeText;
import static java.lang.String.format;

/**
 * Utility class to deal with the agents-file.
 *
 * An agent entry can either be:
 * <ol>
 * <li>
 * a single address.
 * </li>
 * <li>
 * 2 addresses separated with a comma. In this case the first address is the public address, used for ssh. The second
 * address is the private address, which is used in Hazelcast members. The public private separation is needed to deal with
 * clouds like EC2.
 * </li>
 * </ol>
 */
public final class AgentsFile {

    private static final Logger LOGGER = Logger.getLogger(AgentsFile.class);

    private AgentsFile() {
    }

    public static void save(File agentsFile, List<AgentAddress> addresses) {
        StringBuffer sb = new StringBuffer();
        for (AgentAddress agentAddress : addresses) {
            if (agentAddress.publicAddress.equals(agentAddress.privateAddress)) {
                sb.append(agentAddress.publicAddress)
                        .append('\n');
            } else {
                sb.append(agentAddress.publicAddress)
                        .append(',')
                        .append(agentAddress.privateAddress)
                        .append('\n');
            }
        }
        writeText(sb.toString(), agentsFile);
    }

    public static List<AgentAddress> load(File agentFile) {
        String content = fileAsText(agentFile);

        String[] addresses = content.split("\n");
        int lineNumber = 1;
        List<AgentAddress> pairs = new LinkedList<AgentAddress>();
        for (String line : addresses) {
            int indexOfComment = line.indexOf("#");
            if (indexOfComment != -1) {
                line = line.substring(0, indexOfComment);
            }

            line = line.trim();
            if (line.isEmpty()) {
                continue;
            }

            String[] chunks = line.split(",");
            switch (chunks.length) {
                case 1:
                    pairs.add(new AgentAddress(chunks[0], chunks[0]));
                    break;
                case 2:
                    pairs.add(new AgentAddress(chunks[0], chunks[1]));
                    break;
                default:
                    LOGGER.fatal(format("Line %s of file %s is invalid, it should contain 1 or 2 addresses separated by a "
                            + "comma, but contains %s", lineNumber, agentFile, chunks.length));
                    System.exit(1);
                    break;
            }
        }
        return pairs;
    }
}
