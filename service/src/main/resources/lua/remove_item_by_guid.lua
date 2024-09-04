-- removes a list of messages by ID from the cluster, returning the deleted messages
-- returns a list of removed envelopes
--   Note: content may be absent for MRM messages, and for these messages, the caller must update the sharedMrmKey
--         to remove the recipient's reference

local queueKey           = KEYS[1] -- sorted set of Envelopes for a device, by queue-local ID
local queueMetadataKey   = KEYS[2] -- hash of message GUID to queue-local IDs
local queueTotalIndexKey = KEYS[3] -- sorted set of all queues in the shard, by timestamp of oldest message
local messageGuids       = ARGV    -- [list[string]] message GUIDs

local removedMessages = {}

for _, guid in ipairs(messageGuids) do
    local messageId = redis.call("HGET", queueMetadataKey, guid)

    if messageId then
        local envelope = redis.call("ZRANGE", queueKey, messageId, messageId, "BYSCORE", "LIMIT", 0, 1)

        redis.call("ZREMRANGEBYSCORE", queueKey, messageId, messageId)
        redis.call("HDEL", queueMetadataKey, guid)

        if envelope and next(envelope) then
            table.insert(removedMessages, envelope[1])
        end
    end
end

if (redis.call("ZCARD", queueKey) == 0) then
    redis.call("DEL", queueKey)
    redis.call("DEL", queueMetadataKey)
    redis.call("ZREM", queueTotalIndexKey, queueKey)
end

return removedMessages
