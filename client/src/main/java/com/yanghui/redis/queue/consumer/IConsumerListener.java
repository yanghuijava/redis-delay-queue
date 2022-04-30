package com.yanghui.redis.queue.consumer;

import com.yanghui.redis.queue.message.Message;

/**
 * @author yanghui
 */
public interface IConsumerListener {

    /**
     * 消息处理
     * @param message
     * @return
     */
    MessageStatus onMessage(Message message);
}
