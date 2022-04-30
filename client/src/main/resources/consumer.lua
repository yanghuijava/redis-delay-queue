//KEYS[1] topic
//KEYS[2] msgStoreKey
//KEYS[3] list队列
//KEYS[4] pre-topic
//ARGV[1] 当前时间戳
//ARGV[2] 获取条数
local expiredValues = redis.call("ZRANGEBYSCORE",KEYS[1],0,ARGV[1],"limit",0,ARGV[2]);
if #expiredValues > 0 then
    for i, v in ipairs(expiredValues) do
        local msg = redis.call("HGET",KEYS[2],v);
        redis.call("lpush",KEYS[3],msg);
        redis.call('zadd', KEYS[4], ARGV[1],v);
    end;
    redis.call('zrem', KEYS[1], unpack(expiredValues));
end;
local v = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES');
if v[1] ~= nil then
    return v[2];
end
return nil;




