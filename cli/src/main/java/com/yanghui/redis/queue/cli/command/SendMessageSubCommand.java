package com.yanghui.redis.queue.cli.command;

import cn.hutool.core.util.IdUtil;
import com.yanghui.redis.queue.message.Message;
import com.yanghui.redis.queue.producer.RedisProducer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.redisson.api.RedissonClient;

/**
 * @author yanghui
 */
public class SendMessageSubCommand extends AbstractSubCommand implements SubCommand{
    @Override
    public String name() {
        return "sendMessage";
    }

    @Override
    public String desc() {
        return "send delay message";
    }

    @Override
    public Options buildCommandlineOptions(final Options options) {
        Option opt = new Option("d", "delaytime", true, "message delay time");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);

        opt = new Option("m", "message", true, "delay message");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine,Options options) {
        RedissonClient redissonClient = null;
        try{
            redissonClient = super.createClient(commandLine);
            RedisProducer redisProducer = new RedisProducer(redissonClient);


            if(!commandLine.hasOption("t")){
                super.printHelpAndExit(this.name(),"r",options);
            }
            String topic = commandLine.getOptionValue("t").trim();
            if(!commandLine.hasOption("m")){
                super.printHelpAndExit(this.name(),"m",options);
            }
            String body = commandLine.getOptionValue("m").trim();
            if(!commandLine.hasOption("d")){
                super.printHelpAndExit(this.name(),"d",options);
            }
            long delayTie = Long.valueOf(commandLine.getOptionValue("d").trim());
            redisProducer.sendDelayMessage(Message.create(topic,body),delayTie);
            System.out.println("message send success!");
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(redissonClient != null){
                redissonClient.shutdown();
            }
            System.exit(0);
        }
    }
}
