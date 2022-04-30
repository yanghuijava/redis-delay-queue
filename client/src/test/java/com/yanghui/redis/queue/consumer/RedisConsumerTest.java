package com.yanghui.redis.queue.consumer;

import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.yanghui.redis.queue.message.Message;
import com.yanghui.redis.queue.util.TopicUtil;
import org.junit.Test;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.redisson.api.RScript;
import org.redisson.api.RSortedSet;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.ScoredEntry;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RedisConsumerTest {

    @Test
    public void test(){
        String luaText = String.format("if ARGV[1] == 'SUCCESS' then\n" +
                "    redis.call(\"zrem\",KEYS[1],ARGV[2]);\n" +
                "    redis.call(\"hdel\",KEYS[2],ARGV[2]);\n" +
                "    return;\n" +
                "end;\n" +
                "local level = {%s};\n" +
                "local msg = redis.call(\"HGET\",KEYS[2],ARGV[2]);\n" +
                "local msgTable = cjson.decode(msg);\n" +
                "local retryCount = msgTable['retryCount'] + 1;\n" +
                "\n" +
                "if(retryCount <= tonumber(ARGV[4])) then\n" +
                "    msgTable['retryCount'] = retryCount;\n" +
                "    local msg1 = cjson.encode(msgTable);\n" +
                "    redis.call(\"hset\",KEYS[2],ARGV[2],msg1);\n" +
                "    local expireTime = ARGV[3] + level[retryCount] * 1000;\n" +
                "    redis.call(\"zrem\",KEYS[1],ARGV[2]);\n" +
                "    redis.call(\"zadd\",KEYS[3],expireTime,ARGV[2]);\n" +
                "    redis.call(\"PUBLISH\",KEYS[4],expireTime);\n" +
                "else\n" +
                "    redis.call(\"zrem\",KEYS[1],ARGV[2]);\n" +
                "    redis.call(\"zadd\",KEYS[5],ARGV[3],ARGV[2]);\n" +
                "end;","10,30,60,120,180,240,300,360,420,480,540,600,1200,1800,3600,7200");
        RedissonClient client = Redisson.create();
        client.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,luaText,RScript.ReturnType.VALUE,
                Lists.newArrayList(TopicUtil.wrapPreTopic("order_cancel"),
                        TopicUtil.wrapStore("order_cancel"),
                        TopicUtil.wrapTopic("order_cancel"),
                        TopicUtil.wrapChannel("order_cancel"),
                        TopicUtil.wrapDLQ("order_cancel")),
                "DELAY","626d06afc53270370db81f67",System.currentTimeMillis(),16);
    }

    @Test
    public void test1(){
        RedissonClient client = Redisson.create();
        RScoredSortedSet<String> zset =  client.getScoredSortedSet("zset",StringCodec.INSTANCE);
        for(ScoredEntry entry : zset.entryRange(0,9)){
            System.out.println(entry.getValue() + " " + entry.getScore());
        }
    }


    public static void main(String[] args) {
        RedisConsumer redisConsumer = new RedisConsumer();
        redisConsumer.setMaxRetryCount(5);
        redisConsumer.subscribe("order_cancel");
        redisConsumer.registerMessageListener(message -> {
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + "  " + message);
            return MessageStatus.SUCCESS;
        });
        redisConsumer.start();
        System.out.println(redisConsumer.isRunning());
    }
}
