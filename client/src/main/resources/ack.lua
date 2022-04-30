//KEYS[1] pre-topic
//KEYS[2] msgStoreKey
//KEYS[3] topic
//KEYS[4] channel
//KEYS[5] 死信队列
//ARGV[1] 消息处理结果
//ARGV[2] msgID
//ARGV[3] 当前时间
//ARGV[4] 最大重试次数

if ARGV[1] == 'SUCCESS' then
    redis.call("zrem",KEYS[1],ARGV[2]);
    redis.call("hdel",KEYS[2],ARGV[2]);
    return;
end;
local level = {10,30,60,120,180,240,300,360,420,480,540};
local msg = redis.call("HGET",KEYS[2],ARGV[2]);
local msgTable = cjson.decode(msg);
local retryCount = msgTable['retryCount'] + 1;

if(retryCount <= tonumber(ARGV[4])) then
    msgTable['retryCount'] = retryCount;
    local msg1 = cjson.encode(msgTable);
    redis.call("hset",KEYS[2],ARGV[2],msg1);
    local expireTime = ARGV[3] + level[retryCount] * 1000;
    redis.call("zrem",KEYS[1],ARGV[2]);
    redis.call("zadd",KEYS[3],expireTime,ARGV[2]);
    redis.call("PUBLISH",KEYS[4],expireTime);
else
    redis.call("zrem",KEYS[1],ARGV[2]);
    redis.call("zadd",KEYS[5],ARGV[3],ARGV[2]);
end;





