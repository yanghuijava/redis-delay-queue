package com.yanghui.redis.queue.cli.command;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

/**
 * @author yanghui
 */
public interface SubCommand {

    String name();

    String desc();

    Options buildCommandlineOptions(final Options options);

    void execute(final CommandLine commandLine,Options options);
}
