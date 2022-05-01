#### Redis-delay-queue

redis实现延迟队列

#### 特性

* 低延迟：使用redis的zset数据结构和本地时间轮实现，直接从redis中pop超时任务，避免轮询扫描，大大减少了延迟的时间

* 高可用：至少消费一次保障了定时消息一定被消费

  备注：由于redis的本身的特点，redis持久化是异步的，如果发生redis宕机，可能会丢失1s的消息。

#### 快速开始

##### 1、添加maven依赖

```xml
<dependency>
  <groupId>com.yanghui.redis.queue</groupId>
  <artifactId>redis-delay-queue-client</artifactId>
  <version>${最新版本}</version>
</dependency>
```

##### 2、生产端

```java
RedisProducer redisProducer = new RedisProducer();
        redisProducer.sendDelayMessage(Message.create("order_cancel","消息：" + DateUtil.format(new Date(),"yyyy-MM-dd HH:mm:ss.SSS")),1000 * 2);
```

备注：默认创建的`RedisProducer`连接的是本地redis，如果需要指定的redis，创建一个`RedissonClient`即可，本组件依赖`Redisson`;消费端一样，下面不作特殊说明了。

##### 3、消费端

```java
RedisConsumer redisConsumer = new RedisConsumer();
        redisConsumer.setMaxRetryCount(5);
        redisConsumer.subscribe("order_cancel");
        redisConsumer.registerMessageListener(message -> {
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()) + "  " + message);
        return MessageStatus.SUCCESS;
        });
        redisConsumer.start();
```

##### 4、相关配置项

1、`Redisson`的相关配置，参考：https://github.com/redisson/redisson/wiki/%E7%9B%AE%E5%BD%95

2、消费端：

* consumeThreadMin：消费端处理消息线程池最小线程数
* consumeThreadMax：消费端处理消息线程池最大线程数
* maxRetryCount：异常时，消息的最大重试次数，目前只支持16个级别的重试，10,30,60,120,180,240,300,360,420,480,540,600,1200,1800,3600,7200，单位秒，此值只能小于等于16，默认16次，超过重试次数，即进入死信队列。

#### 实现原理

<img src="https://raw.githubusercontent.com/yanghuijava/redis-delay-queue/main/images/redis%E5%BB%B6%E8%BF%9F%E9%98%9F%E5%88%97.png" style="zoom:100%;" />


