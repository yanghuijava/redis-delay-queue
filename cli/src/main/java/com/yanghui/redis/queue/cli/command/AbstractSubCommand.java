package com.yanghui.redis.queue.cli.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

/**
 * @author yanghui
 */
public abstract class AbstractSubCommand {

    protected RedissonClient createClient(CommandLine commandLine){
        String redisServer;
        if(commandLine.hasOption("r")){
            redisServer = commandLine.getOptionValue("r").trim();
        }else {
            return Redisson.create();
        }
        Config config = new Config();
        config.useSingleServer().setAddress("redis://" + redisServer);
        if(commandLine.hasOption("p")){
            config.useSingleServer().setPassword(commandLine.getOptionValue("p").trim());
        }
        return Redisson.create(config);
    }

    protected void printHelpAndExit(String name, String param,Options options){
        System.out.printf("missing 【%s】 parameter\n",param);
        CommandUtil.printHelp(name,options);
        System.exit(0);
    }
}
