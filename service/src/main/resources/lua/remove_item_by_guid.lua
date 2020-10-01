local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]

local removedMessages = {}

for _, guid in ipairs(ARGV) do
    local messageId = redis.call("HGET", queueMetadataKey, guid)

    if messageId then
        local envelope = redis.call("ZRANGEBYSCORE", queueKey, messageId, messageId, "LIMIT", 0, 1)
        local sender   = redis.call("HGET", queueMetadataKey, messageId)

        redis.call("ZREMRANGEBYSCORE", queueKey, messageId, messageId)
        redis.call("HDEL", queueMetadataKey, guid)
        redis.call("HDEL", queueMetadataKey, messageId .. "guid")

        if sender then
            redis.call("HDEL", queueMetadataKey, sender)
            redis.call("HDEL", queueMetadataKey, messageId)
        end

        if (redis.call("ZCARD", queueKey) == 0) then
            redis.call("ZREM", queueTotalIndexKey, queueKey)
        end

        if envelope and next(envelope) then
            removedMessages[#removedMessages + 1] = envelope[1]
        end
    end
end

return removedMessages
