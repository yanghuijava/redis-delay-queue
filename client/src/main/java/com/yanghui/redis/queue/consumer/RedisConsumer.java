package com.yanghui.redis.queue.consumer;

import cn.hutool.core.lang.Assert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.collect.Lists;
import com.yanghui.redis.queue.common.ThreadFactoryImpl;
import com.yanghui.redis.queue.message.Message;
import com.yanghui.redis.queue.util.TopicUtil;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import lombok.Getter;
import lombok.Setter;
import org.redisson.Redisson;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author yanghui
 */
public class RedisConsumer implements IConsumer{

    private RedissonClient redissonClient;

    private String topic;

    private volatile boolean isRunning = false;

    private HashedWheelTimer hashedWheelTimer;

    private ConcurrentHashMap<Long, Timeout> timeoutMap = new ConcurrentHashMap<>();

    private ThreadPoolExecutor messageHandleExecutor;

    private IConsumerListener consumerListener;

    @Setter
    @Getter
    private int consumeThreadMin = 20;

    @Getter
    @Setter
    private int consumeThreadMax = 20;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ScheduledExecutorService clearTimeoutMapScheduledExecutorService;

    private final ScheduledExecutorService preTopicExceptionExecutorService;

    private String channelKey;

    private String listKey;

    private String storeKey;

    private String preTopicKey;

    private String dlqKey;

    private String topicKey;

    public void subscribe(String topic){
        Assert.notBlank(topic,"topic is not null");
        this.topic = topic;
        this.topicKey = TopicUtil.wrapTopic(this.topic);
        this.channelKey = TopicUtil.wrapChannel(this.topic);
        this.listKey = TopicUtil.wrapList(this.topic);
        this.storeKey = TopicUtil.wrapStore(this.topic);
        this.preTopicKey = TopicUtil.wrapPreTopic(this.topic);
        this.dlqKey = TopicUtil.wrapDLQ(this.topic);
    }

    public RedisConsumer(){
        this(Redisson.create());
    }

    public RedisConsumer(RedissonClient redissonClient){
        this.redissonClient = redissonClient;
        this.hashedWheelTimer = new HashedWheelTimer();
        this.scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("PollMessageScheduledThread_"));
        this.clearTimeoutMapScheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("cleartimeoutMapScheduledThread_"));
        this.preTopicExceptionExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("preTopicExceptionExecutorThread_"));
        this.messageHandleExecutor = new ThreadPoolExecutor(
                this.consumeThreadMin,
                this.consumeThreadMax,
                1000 * 60,
                TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(),
                new ThreadFactoryImpl("ConsumeMessageThread_"));

    }

    public void registerMessageListener(IConsumerListener consumerListener){
        this.consumerListener = consumerListener;
    }

    @Override
    public synchronized void start() {
        Assert.notBlank(this.topic,"topic is not null");
        Assert.notNull(consumerListener,"consumerListener is not null");
        if(isRunning){
            return;
        }
        /** 监听channel消息 **/
        this.redissonClient.getTopic(channelKey, StringCodec.INSTANCE).addListener(String.class, (channel, msg) -> {
            /** 获取订阅channel消息，内容为延迟消息的到期时间戳 **/
            createTimeout(Long.valueOf(msg));
        });

        /** 监听消息 **/
        this.scheduledExecutorService.submit((Runnable) () -> {
            while(true){
                try {
                    String messageJson = (String)redissonClient.getBlockingQueue(listKey,StringCodec.INSTANCE)
                            .poll(1000 * 3,TimeUnit.MILLISECONDS);
                    if(StrUtil.isEmpty(messageJson)){
                        Thread.sleep(2);
                        continue;
                    }
                    if(!JSONUtil.isTypeJSON(messageJson)){
                        continue;
                    }
                    Message message = JSONUtil.toBean(messageJson,Message.class);
                    messageHandleExecutor.submit(() -> handlerMessage(message));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        clearTimeoutMapScheduledExecutorService.scheduleAtFixedRate(() -> {
            try{
                List<Long> removeKeys = Lists.newArrayList();
                timeoutMap.forEach((k,v) -> {
                    if(k < System.currentTimeMillis()){
                        removeKeys.add(k);
                    }
                });
                if(!removeKeys.isEmpty()){
                    removeKeys.forEach(i -> timeoutMap.remove(i));
                }
            }catch (Exception e){
                e.printStackTrace();
            }
        },2,10,TimeUnit.SECONDS);

        this.preTopicExceptionExecutorService.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {

            }
        },2,15,TimeUnit.SECONDS);
        this.isRunning = true;
        /** 启动后 执行一次 任务推送 **/
        pushTask();
    }

    private void pushTask(){
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
        String expireTime = this.redissonClient.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,luaText, RScript.ReturnType.VALUE,
                Lists.newArrayList(this.topicKey,this.storeKey,listKey,preTopicKey),
                System.currentTimeMillis(),100);
        if(!StrUtil.isEmpty(expireTime)){
            this.createTimeout(Long.valueOf(expireTime));
        }
    }

    private void handlerMessage(Message message){
        MessageStatus messageStatus = null;
        try{
            messageStatus  = this.consumerListener.onMessage(message);
        }catch (Exception e){
            e.printStackTrace();
        }
        if(messageStatus == null){
            messageStatus = MessageStatus.DELAY;
        }
        ack(message,messageStatus);
    }

    private void ack(Message message,MessageStatus messageStatus){
        String luaText = "if ARGV[1] == 'SUCCESS' then\n" +
                "    redis.call(\"zrem\",KEYS[1],ARGV[2]);\n" +
                "    redis.call(\"hdel\",KEYS[2],ARGV[2]);\n" +
                "    return;\n" +
                "end;\n" +
                "local level = {10,30,60,120,180,240,300,360,420,480,540}\n" +
                "local msg = redis.call(\"HGET\",KEYS[2],ARGV[2]);\n" +
                "local msgTable = cjson.decode(msg);\n" +
                "local retryCount = msgTable['retryCount'] + 1;\n" +
                "msgTable['retryCount'] = retryCount;\n" +
                "local msg1 = cjson.encode(msgTable);\n" +
                "redis.call(\"hset\",KEYS[2],ARGV[2],msg1);\n" +
                "\n" +
                "redis.call(\"zrem\",KEYS[1],ARGV[2]);\n" +
                "if(retryCount <= 11) then\n" +
                "    local expireTime = ARGV[3] + level[retryCount] * 1000;\n" +
                "    redis.call(\"zadd\",KEYS[3],expireTime,ARGV[2]);\n" +
                "    redis.call(\"PUBLISH\",KEYS[4],expireTime);\n" +
                "else\n" +
                "    redis.call(\"lpush\",KEYS[5],ARGV[2]);\n" +
                "end;\n";
        this.redissonClient.getScript(StringCodec.INSTANCE).eval(RScript.Mode.READ_WRITE,luaText,RScript.ReturnType.VALUE,
                Lists.newArrayList(this.preTopicKey,this.storeKey,this.topicKey,this.channelKey,this.dlqKey),
                messageStatus.name(),message.getId(),System.currentTimeMillis());
    }

    private synchronized void createTimeout(long expireTime){
        if(this.timeoutMap.get(expireTime) != null){
            return;
        }
        long delay = expireTime - System.currentTimeMillis();
        /** 如果剩余时间小于10毫秒 直接执行**/
        if(delay <= 10){
            pushTask();
        }else {
            Timeout timeout1 = this.hashedWheelTimer.newTimeout(timeout -> pushTask(),delay, TimeUnit.MILLISECONDS);
            timeoutMap.putIfAbsent(expireTime,timeout1);
        }
    }

    @Override
    public synchronized void stop() {
        if(!this.isRunning){
            return;
        }
        this.redissonClient.shutdown();
        this.hashedWheelTimer.stop();
        this.messageHandleExecutor.shutdown();
        this.scheduledExecutorService.shutdown();
        this.clearTimeoutMapScheduledExecutorService.shutdown();
        this.isRunning = false;
    }

    @Override
    public boolean isRunning() {
        return this.isRunning;
    }
}
