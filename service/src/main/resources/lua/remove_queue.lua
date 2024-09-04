-- incrementally removes a given device's queue and associated data
-- returns: a page of messages and scores.
--    The messages must be checked for mrmKeys to update. After updating MRM keys, this script must be called again
--    with processedMessageGuids. If the returned table is empty, then
--    the queue has been fully deleted.

local queueKey              = KEYS[1] -- sorted set of Envelopes for a device, by queue-local ID
local queueMetadataKey      = KEYS[2] -- hash of message GUID to queue-local IDs
local queueTotalIndexKey    = KEYS[3] -- sorted set of all queues in the shard, by timestamp of oldest message
local limit                 = ARGV[1] -- the maximum number of messages to return
local processedMessageGuids = { unpack(ARGV, 2) }

for _, guid in ipairs(processedMessageGuids) do
    local messageId = redis.call("HGET", queueMetadataKey, guid)
    if messageId then
        redis.call("ZREMRANGEBYSCORE", queueKey, messageId, messageId)
        redis.call("HDEL", queueMetadataKey, guid)
    end
end

local messages = redis.call("ZRANGE", queueKey, 0, limit-1)

if #messages == 0 then
    redis.call("DEL", queueKey)
    redis.call("DEL", queueMetadataKey)
    redis.call("ZREM", queueTotalIndexKey, queueKey)
end

return messages
