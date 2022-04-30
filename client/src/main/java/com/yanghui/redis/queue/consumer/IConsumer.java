package com.yanghui.redis.queue.consumer;

/**
 * @author yanghui
 */
public interface IConsumer {

    /**
     * 启动消费者
     */
    void start();

    /**
     * 停止消费者
     */
    void stop();

    /**
     * 消费者是否运行状态
     * @return
     */
    boolean isRunning();
}
