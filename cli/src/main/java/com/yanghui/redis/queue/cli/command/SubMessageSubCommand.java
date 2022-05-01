package com.yanghui.redis.queue.cli.command;

import cn.hutool.core.util.IdUtil;
import com.yanghui.redis.queue.consumer.IConsumerListener;
import com.yanghui.redis.queue.consumer.MessageStatus;
import com.yanghui.redis.queue.consumer.RedisConsumer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yanghui
 */
public class SubMessageSubCommand extends AbstractSubCommand implements SubCommand{
    @Override
    public String name() {
        return "subMessage";
    }

    @Override
    public String desc() {
        return "subscription message";
    }

    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("t", "topic", true, "topic name");
        opt.setRequired(false);
        options.addOption(opt);
        return options;
    }

    @Override
    public void execute(CommandLine commandLine, Options options) {
        RedisConsumer redisConsumer = null;
        try{
            if(!commandLine.hasOption("t")){
                super.printHelpAndExit(this.name(),"r",options);
            }
            redisConsumer = new RedisConsumer(super.createClient(commandLine));
            redisConsumer.subscribe(commandLine.getOptionValue("t").trim());
            redisConsumer.registerMessageListener(message -> {
                System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + " " + message);
                return MessageStatus.SUCCESS;
            });
            redisConsumer.start();
            System.out.println("consumer start......");
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
