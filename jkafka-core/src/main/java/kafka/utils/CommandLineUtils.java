package kafka.utils;

import java.io.IOException;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Helper functions for dealing with command line utilities
 */
public abstract class CommandLineUtils {
    public static void checkRequiredArgs(OptionParser parser, OptionSet options, OptionSpec... required) {
        for (OptionSpec arg : required) {
            if (!options.has(arg)) {
                System.err.println("Missing required argument \"" + arg + "\"");
                try {
                    parser.printHelpOn(System.err);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.exit(1);
            }
        }
    }
}
