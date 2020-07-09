-- keys: queue_key, queue_metadata_key, queue_index
-- argv: index_to_remove

local envelope     = redis.call("ZRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1], "LIMIT", 0, 1)
local removedCount = redis.call("ZREMRANGEBYSCORE", KEYS[1], ARGV[1], ARGV[1])
local senderIndex  = redis.call("HGET", KEYS[2], ARGV[1])
local guidIndex    = redis.call("HGET", KEYS[2], ARGV[1] .. "guid")

if senderIndex then
    redis.call("HDEL", KEYS[2], senderIndex)
    redis.call("HDEL", KEYS[2], ARGV[1])
end

if guidIndex then
    redis.call("HDEL", KEYS[2], guidIndex)
    redis.call("HDEL", KEYS[2], ARGV[1] .. "guid")
end

if (redis.call("ZCARD", KEYS[1]) == 0) then
    redis.call("ZREM", KEYS[3], KEYS[1])
end

if envelope and next(envelope) then
    return envelope[1]
else
    return nil
end
