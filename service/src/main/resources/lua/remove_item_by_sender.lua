local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]
local sender             = ARGV[1]

local messageId = redis.call("HGET", queueMetadataKey, sender)

if messageId then
    local envelope = redis.call("ZRANGEBYSCORE", queueKey, messageId, messageId, "LIMIT", 0, 1)
    local guid     = redis.call("HGET", queueMetadataKey, messageId .. "guid")

    redis.call("ZREMRANGEBYSCORE", queueKey, messageId, messageId)
    redis.call("HDEL", queueMetadataKey, sender)
    redis.call("HDEL", queueMetadataKey, messageId)

    if guid then
        redis.call("HDEL", queueMetadataKey, guid)
        redis.call("HDEL", queueMetadataKey, messageId .. "guid")
    end

    if (redis.call("ZCARD", queueKey) == 0) then
        redis.call("ZREM", queueTotalIndexKey, queueKey)
    end

    if envelope and next(envelope) then
        return envelope[1]
    end
end

return nil
