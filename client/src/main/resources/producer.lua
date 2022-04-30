//KEYS[1] topic
//KEYS[2] msgStoreKey
//ARGV[1] 到期时间
//ARGV[2] msgId
//ARGV[3] msg
redis.call("zadd",KEYS[1],ARGV[1],ARGV[2]);
redis.call("hset",KEYS[2],ARGV[2],ARGV[3]);
redis.call("PUBLISH",KEYS[3],ARGV[1]);