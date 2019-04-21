-- keys: pending (KEYS[1]), user (KEYS[2])
-- args: timestamp (ARGV[1]), interval (ARGV[2]), account (ARGV[3]), device (ARGV[4])

redis.call("HSET", KEYS[2], "created", ARGV[1])
redis.call("HSET", KEYS[2], "interval", ARGV[2])
redis.call("HSET", KEYS[2], "account", ARGV[3])
redis.call("HSET", KEYS[2], "device", ARGV[4])

redis.call("ZADD", KEYS[1], ARGV[1], KEYS[2])
