package com.yanghui.redis.queue.producer;


import cn.hutool.core.lang.Assert;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.yanghui.redis.queue.message.Message;
import com.yanghui.redis.queue.util.TopicUtil;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

/**
 * @author yanghui
 */
public class RedisProducer implements IProducer{

    private RedissonClient redissonClient;

    public RedisProducer(){
        this.redissonClient = Redisson.create();
    }

    public RedisProducer(RedissonClient redissonClient){
        this.redissonClient = redissonClient;
    }

    @Override
    public void sendDelayMessage(Message message, long delayTime) {
        Assert.notNull(message,"message not null");
        Assert.isTrue(delayTime > 0,"delayTime must be greater than 0");
        Assert.notBlank(message.getBody(),"message body not null");
        Assert.notBlank(message.getTopic(),"message topic not null");
        Assert.notBlank(message.getId(),"message id not null");
        if(message.getRetryCount() == null){
            message.setRetryCount(0);
        }
        String topicKey = TopicUtil.wrapTopic(message.getTopic());
        String msgStoreKey = TopicUtil.wrapStore(message.getTopic());
        String channelKey = TopicUtil.wrapChannel(message.getTopic());
        String luaText = "redis.call('zadd',KEYS[1],ARGV[1],ARGV[2]);" +
                        "redis.call('hset',KEYS[2],ARGV[2],ARGV[3]);" +
                        "redis.call('PUBLISH',KEYS[3],ARGV[1]);";
        this.redissonClient.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,luaText,RScript.ReturnType.VALUE,
                Lists.newArrayList(topicKey,msgStoreKey,channelKey),
                System.currentTimeMillis() + delayTime,
                message.getId(),
                JSONUtil.toJsonStr(message));
    }
}
