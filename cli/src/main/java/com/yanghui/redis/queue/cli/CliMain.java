package com.yanghui.redis.queue.cli;


import com.google.common.collect.Maps;
import com.yanghui.redis.queue.cli.command.CommandUtil;
import com.yanghui.redis.queue.cli.command.SendMessageSubCommand;
import com.yanghui.redis.queue.cli.command.SubCommand;
import com.yanghui.redis.queue.cli.command.SubMessageSubCommand;
import org.apache.commons.cli.*;

import java.util.Map;

/**
 * @author yanghui
 */
public class CliMain {

    private static Map<String, SubCommand> subCommandMap = Maps.newHashMap();

    public static void main(String[] args) {
        main0(args);
    }

    private static void main0(String[] args){
        initCommand();

        Options options = CommandUtil.buildCommandlineOptions(new Options());
        switch (args.length){
            case 0:
                printHelp(options);
                break;
            case 1:
                String command = CommandUtil.parseCommand(args);
                if(command.equals("-h") || command.equals("--help") || command.equals("help") || command.equals("h")){
                    printHelp(options);
                    return;
                }
                SubCommand subCommand = subCommandMap.get(command);
                if(subCommand != null){
                    CommandUtil.printHelp(subCommand.name(),subCommand.buildCommandlineOptions(options));
                }else {
                    System.out.printf("subCommand:{%s} not exist!\n",command);
                    printHelp(options);
                }
                break;
            default:
                command = CommandUtil.parseCommand(args);
                subCommand = subCommandMap.get(command);
                String[] subArgs = CommandUtil.parseSubArgs(args);
                if(subCommand == null){
                    System.out.printf("subCommand:{%s} not exist!\n",command);
                    printHelp(options);
                    return;
                }
                CommandLine commandLine = CommandUtil.parseCmdLine(subCommand.name(),subArgs,
                        subCommand.buildCommandlineOptions(options),new DefaultParser());
                subCommand.execute(commandLine,options);
                break;
        }
    }

    private static void printHelp(Options options){
        for(Map.Entry<String, SubCommand> entry : subCommandMap.entrySet()){
            CommandUtil.printHelp(entry.getKey(),entry.getValue().buildCommandlineOptions(options));
        }
        System.exit(0);
    }


    private static void initCommand(){
        initCommand(new SendMessageSubCommand());
        initCommand(new SubMessageSubCommand());
    }

    private static void initCommand(SubCommand subCommand) {
        subCommandMap.put(subCommand.name(), subCommand);
    }
}
