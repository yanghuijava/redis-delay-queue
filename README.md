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

##### 5、命令行操作

为了方面体验延迟队列的功能，这里提供一个命令行工具。

可以通过命令来查看所有的参数说明：

```
java -jar cli.jar -h
```

* 发送消息

  ```shell
  java -jar cli.jar sendMessage -r 127.0.0.1:6379 -t order_cancel -m 消息1 -d 5000
  ```

* 消费消息

  ```shell
  java -jar cli.jar subMessage -r 127.0.0.1:6379 -t order_cancel
  ```

#### 实现原理

<img src="https://raw.githubusercontent.com/yanghuijava/redis-delay-queue/main/images/redis%E5%BB%B6%E8%BF%9F%E9%98%9F%E5%88%97.png" style="zoom:100%;" />

这里二阶段消费原理参考阿里的一篇文章：https://www.toutiao.com/article/6969810934080684551/?app=news_article&timestamp=1650897110&use_new_style=1&req_id=20220425223150010133037132012BF348&group_id=6969810934080684551&share_token=50D46337-0304-415F-ADCB-F5534F04DF83&tt_from=weixin&utm_source=weixin&utm_medium=toutiao_ios&utm_campaign=client_share&wxshare_count=1

##### Topic设计

topic是逻辑概念，这里对应redis的key有5个，分别的作用如下：

* `redis_delay_queue_origin:{Topic名称}`，对应redis的`zset`数据结构，value表示消息的ID，score表示消息的到期执行时间戳，会有定时任务来扫描到期的消息
* `redis_delay_queue_store:{Topic名称}`，对应redis的`hash`数据结构，hash的key表示消息ID，value表示消息的内容
* `redis_delay_queue_list:{Topic名称}`，对应redis的`list`数据结构，存储的是消息ID，每当消息到期时间到了，`redis_delay_queue_origin:{Topic名称}`移动到当前`list`中，消费者会监听这个`list`。
* `redis_delay_queue_pre:{Topic名称}`，对应redis的`zset`数据结构，为了保证每条消息的至少消费一次，每次把到期消息移动至`redis_delay_queue_list:{Topic名称}`外，这里还会放置到当前队列中，等业务真正消费完成，进行ack后，再删除。

* `redis_delay_queue_DLQ:{Topic名称}`，对应redis的`zset`数据结构，消息一旦超过重试次数，即会进入死信队列。

##### Producer端

![](https://github.com/yanghuijava/redis-delay-queue/blob/main/images/redisProducer.png?raw=true)

##### Consumer端

![](https://github.com/yanghuijava/redis-delay-queue/blob/main/images/redisConsumer.png?raw=true)

##### 异常处理

如何消息ack失败，消息会一直处于`redis_delay_queue_pre:{Topic名称}`队列中，而无法消费了，这时会启动一个定时任务，定时扫描`redis_delay_queue_pre:{Topic名称}`，把时间超过3分钟没有ack的消息重新发布到`redis_delay_queue_origin:{Topic名称}`队列中，让消息重新消费。