local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]
local id                 = ARGV[1]

local envelope     = redis.call("ZRANGEBYSCORE", queueKey, id, id, "LIMIT", 0, 1)
local removedCount = redis.call("ZREMRANGEBYSCORE", queueKey, id, id)
local senderIndex  = redis.call("HGET", queueMetadataKey, id)
local guidIndex    = redis.call("HGET", queueMetadataKey, id .. "guid")

if senderIndex then
    redis.call("HDEL", queueMetadataKey, senderIndex)
    redis.call("HDEL", queueMetadataKey, id)
end

if guidIndex then
    redis.call("HDEL", queueMetadataKey, guidIndex)
    redis.call("HDEL", queueMetadataKey, id .. "guid")
end

if (redis.call("ZCARD", queueKey) == 0) then
    redis.call("ZREM", queueTotalIndexKey, queueKey)
end

if envelope and next(envelope) then
    return envelope[1]
else
    return nil
end
