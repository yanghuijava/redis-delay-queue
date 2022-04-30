package com.yanghui.redis.queue.util;

/**
 * @author yanghui
 */
public class TopicUtil {

    public static final String PREFIX = "redis_delay_queue_origin:";

    public static final String PREFIX_STORE = "redis_delay_queue_store:";

    public static final String PREFIX_CHANNEL = "redis_delay_queue_channel:";

    public static final String PREFIX_LIST = "redis_delay_queue_list:";

    public static final String PREFIX_PRE = "redis_delay_queue_pre:";

    public static final String PREFIX_DLQ = "redis_delay_queue_DLQ:";

    public static String wrapTopic(String topic){
        return PREFIX + "{" + topic + "}";
    }

    public static String wrapStore(String topic){
        return PREFIX_STORE + "{" + topic + "}";
    }

    public static String wrapChannel(String topic){
        return PREFIX_CHANNEL + "{" + topic + "}";
    }

    public static String wrapList(String topic){
        return PREFIX_LIST + "{" + topic + "}";
    }

    public static String wrapPreTopic(String topic){
        return PREFIX_PRE + "{" + topic + "}";
    }

    public static String wrapDLQ(String topic){
        return PREFIX_DLQ + "{" + topic + "}";
    }


}
