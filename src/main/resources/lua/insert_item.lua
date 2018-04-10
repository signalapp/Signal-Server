-- keys: queue_key, queue_metadata_key, queue_total_index
-- argv: message, current_time, sender_key

local messageId = redis.call("HINCRBY", KEYS[2], "counter", 1)
redis.call("ZADD", KEYS[1], "NX", messageId, ARGV[1])
redis.call("HSET", KEYS[2], ARGV[3], messageId)
redis.call("HSET", KEYS[2], messageId, ARGV[3])

redis.call("EXPIRE", KEYS[1], 7776000)
redis.call("EXPIRE", KEYS[2], 7776000)

redis.call("ZADD", KEYS[3], "NX", ARGV[2], KEYS[1])
return messageId