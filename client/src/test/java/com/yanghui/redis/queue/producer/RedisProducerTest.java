package com.yanghui.redis.queue.producer;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.IdUtil;
import com.google.common.collect.Lists;
import com.yanghui.redis.queue.message.Message;
import com.yanghui.redis.queue.util.TopicUtil;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.ObjectListener;
import org.redisson.api.RScript;
import org.redisson.api.listener.MessageListener;
import org.redisson.api.listener.StatusListener;
import org.redisson.client.codec.StringCodec;

import java.util.Date;
import java.util.concurrent.TimeUnit;

public class RedisProducerTest {

    @Test
    public void test1(){
        RedisProducer redisProducer = new RedisProducer();
        redisProducer.sendDelayMessage(Message.create("order_cancel","消息："
                + DateUtil.format(new Date(),"yyyy-MM-dd HH:mm:ss.SSS")),1000 * 10);
    }

    @Test
    public void test2(){
        Object result = Redisson.create().getScript(StringCodec.INSTANCE).eval(
                RScript.Mode.READ_WRITE,"redis.call('zadd',KEYS[1],ARGV[1],ARGV[2]);",RScript.ReturnType.VALUE,
                Lists.newArrayList("redis_delay_queue:{order_cancel}"),System.currentTimeMillis(), IdUtil.objectId());
        System.out.println(result);
    }

    @Test
    public void test3() throws InterruptedException {
        Redisson.create().getTopic("redis_delay_queue_channel:{order_cancel}",StringCodec.INSTANCE)
                .addListener(String.class,new MessageListener<String>(){
                    @Override
                    public void onMessage(CharSequence channel, String msg) {
                        System.out.println(msg);
                    }
                });
        Thread.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void test4() throws InterruptedException {
        while (true){
            String re = (String)Redisson.create().getBlockingQueue("list",StringCodec.INSTANCE).poll(1000 * 5, TimeUnit.MILLISECONDS);
            System.out.println(re);
        }
    }

    @Test
    public void test6() throws InterruptedException {
        String luaText = "local expiredValues = redis.call(\"ZRANGEBYSCORE\",KEYS[1],0,ARGV[1],\"limit\",0,ARGV[2]);\n" +
                "if #expiredValues > 0 then\n" +
                "    for i, v in ipairs(expiredValues) do\n" +
                "        local msg = redis.call(\"HGET\",KEYS[2],v);\n" +
                "        redis.call(\"rpush\",KEYS[3],msg);\n" +
                "        redis.call('zadd', KEYS[4], ARGV[1],v);\n" +
                "    end;\n" +
                "    redis.call('zrem', KEYS[1], unpack(expiredValues));\n" +
                "end;\n" +
                "local v = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES');\n" +
                "if v[1] ~= nil then\n" +
                "    return v[2];\n" +
                "end\n" +
                "return nil;";
        String expireTime = Redisson.create().getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,luaText, RScript.ReturnType.VALUE,
                Lists.newArrayList(TopicUtil.wrapTopic("order_cancel"),
                        TopicUtil.wrapStore("order_cancel"),
                        TopicUtil.wrapList("order_cancel"),
                        TopicUtil.wrapPreTopic("order_cancel")),
                System.currentTimeMillis(),100);
        System.out.println(expireTime);
    }

    @Test
    public void test7(){
        String lua = "local values = redis.call('ZRANGEBYSCORE',KEYS[1],0,ARGV[1],'limit',0,100,'withscores')" +
                "return values";
        Object result = Redisson.create().getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,lua, RScript.ReturnType.VALUE,
                Lists.newArrayList("zset"),System.currentTimeMillis());
        System.out.println(result);
    }
}
