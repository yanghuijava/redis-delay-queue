package com.yanghui.redis.queue.cli.command;

import org.apache.commons.cli.*;

/**
 * @author yanghui
 */
public class CommandUtil {

    public static String parseCommand(String[] args){
        if(args.length <= 0){
            return null;
        }
        return args[0];
    }

    public static String[] parseSubArgs(String[] args){
        if (args.length > 1) {
            String[] result = new String[args.length - 1];
            for (int i = 0; i < args.length - 1; i++) {
                result[i] = args[i + 1];
            }
            return result;
        }
        return null;
    }

    public static Options buildCommandlineOptions(Options options){
        Option opt = new Option("h", "help", false, "Print help");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("r", "redisAddress", true, "redis server address list, eg: '192.168.0.1:6379'");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("p", "redisPassword", true, "redis password");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }

    public static CommandLine parseCmdLine(String appName, String[] args, Options options, CommandLineParser parser){
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.hasOption('h')) {
                hf.printHelp(appName, options, true);
                System.exit(0);
            }
        } catch (ParseException e) {
            hf.printHelp(appName, options, true);
            System.exit(1);
        }
        return commandLine;
    }

    public static void printHelp(String name,Options options){
        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        hf.printHelp(name,options,true);
    }
}
