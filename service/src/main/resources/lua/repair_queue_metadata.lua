local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]

local firstMessageWithScore = redis.call("ZRANGE", queueKey, 0, 0, "WITHSCORES")
local lastMessageWithScore  = redis.call("ZRANGE", queueKey, -1, -1, "WITHSCORES")

if firstMessageWithScore ~= nil and lastMessageWithScore ~= nil then
    local firstMessageId = tonumber(firstMessageWithScore[2])
    local lastMessageId  = tonumber(lastMessageWithScore[2])

    for messageId = firstMessageId,lastMessageId do
        if redis.call("ZRANGEBYSCORE", queueKey, messageId, messageId) then
            -- This message actually exists, and its GUID may be pointing to the wrong ID
            local guid = redis.call("HGET", queueMetadataKey, messageId .. "guid")
            redis.call("HSET", queueMetadataKey, guid, messageId)
        else
            -- No message actually exists with that ID; drop the metadata reference to that ID
            redis.call("HDEL", queueMetadataKey, messageId .. "guid")
        end
    end
end
