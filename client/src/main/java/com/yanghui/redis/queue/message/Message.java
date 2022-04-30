package com.yanghui.redis.queue.message;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.IdUtil;
import lombok.Data;
import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yanghui
 */
@Data
public class Message {

    private String id;

    private String topic;

    private String body;

    private Map<String,String> headers = new HashMap<>();

    private Integer retryCount;

    public Message(){}

    public Message(String topic,String body){
        Assert.notBlank(topic,"topic not empty");
        Assert.notBlank(body,"body not empty");
        this.topic = topic;
        this.body = body;
        this.id = IdUtil.objectId();
        this.retryCount = 0;
    }

    public void addHeader(String key,String value){
        Assert.notBlank(key,"key not empty");
        Assert.notBlank(value,"value not empty");
        this.headers.put(key,value);
    }

    public String getHeader(String key){
        return this.headers.get(key);
    }

    public static Message create(String topic,String body){
        Message message = new Message(topic,body);
        return message;
    }
}
