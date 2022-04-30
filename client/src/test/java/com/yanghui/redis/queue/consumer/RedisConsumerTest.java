package com.yanghui.redis.queue.consumer;

import cn.hutool.json.JSONUtil;
import com.yanghui.redis.queue.message.Message;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Date;

public class RedisConsumerTest {

    @Test
    public void test(){
        String me = "{\n" +
                "  \"headers\": {},\n" +
                "  \"retryCount\": 0,\n" +
                "  \"body\": \"消息：2022-04-29 19:09:58.166\",\n" +
                "  \"topic\": \"order_cancel\",\n" +
                "  \"id\": \"626bc786c5324f673a76d335\"\n" +
                "}";
        Message message = JSONUtil.toBean(me,Message.class);
        System.out.println(message);
    }

    public static void main(String[] args) {
        RedisConsumer redisConsumer = new RedisConsumer();
        redisConsumer.subscribe("order_cancel");
        redisConsumer.registerMessageListener(message -> {
            System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + "  " + message);
            return MessageStatus.SUCCESS;
        });
        redisConsumer.start();
        System.out.println(redisConsumer.isRunning());
    }
}
