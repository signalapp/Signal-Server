local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]

local removedMessages = {}

for _, guid in ipairs(ARGV) do
    local messageId = redis.call("HGET", queueMetadataKey, guid)

    if messageId then
        local envelope = redis.call("ZRANGEBYSCORE", queueKey, messageId, messageId, "LIMIT", 0, 1)

        redis.call("ZREMRANGEBYSCORE", queueKey, messageId, messageId)
        redis.call("HDEL", queueMetadataKey, guid)

        if envelope and next(envelope) then
            removedMessages[#removedMessages + 1] = envelope[1]
        end
    end
end

if (redis.call("ZCARD", queueKey) == 0) then
    redis.call("DEL", queueKey)
    redis.call("DEL", queueMetadataKey)
    redis.call("ZREM", queueTotalIndexKey, queueKey)
end

return removedMessages
