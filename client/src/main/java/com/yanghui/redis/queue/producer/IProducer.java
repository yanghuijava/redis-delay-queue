package com.yanghui.redis.queue.producer;

import com.yanghui.redis.queue.message.Message;

/**
 * @author yanghui
 */
public interface IProducer {

    /**
     * 发送延迟消息
     * @param message 消息
     * @param delayTime 延迟时间（单位：毫秒）
     */
    void sendDelayMessage(Message message, long delayTime);

}
