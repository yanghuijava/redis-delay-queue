//KEYS[1] pre-topic

//ARGV[1] 当前时间戳

local values = redis.call('ZRANGEBYSCORE',KEYS[1],0,ARGV[1],'limit',0,100,'withscores');
if #values > 0 then
    for i, v in ipairs(values) do
        return v[0]
    end;
end;