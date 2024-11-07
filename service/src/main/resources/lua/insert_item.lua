-- inserts a message into a device's queue, and updates relevant associated data
-- returns a number, the queue-local message ID (useful for testing)

local queueKey           = KEYS[1] -- sorted set of Envelopes for a device, by queue-local ID
local queueMetadataKey   = KEYS[2] -- hash of message GUID to queue-local IDs
local queueTotalIndexKey = KEYS[3] -- sorted set of all queues in the shard, by timestamp of oldest message
local eventChannelKey    = KEYS[4] -- pub/sub channel for message availability events
local message            = ARGV[1] -- [bytes] the Envelope to insert
local currentTime        = ARGV[2] -- [number] the message timestamp, to sort the queue in the queueTotalIndex
local guid               = ARGV[3] -- [string] the message GUID
local eventPayload       = ARGV[4] -- [bytes] a protobuf payload for a "message available" pub/sub event

if redis.call("HEXISTS", queueMetadataKey, guid) == 1 then
    return tonumber(redis.call("HGET", queueMetadataKey, guid))
end

local messageId = redis.call("HINCRBY", queueMetadataKey, "counter", 1)

redis.call("ZADD", queueKey, "NX", messageId, message)

redis.call("HSET", queueMetadataKey, guid, messageId)
redis.call("EXPIRE", queueKey, 3974400) -- 46 days
redis.call("EXPIRE", queueMetadataKey, 3974400) -- 46 days

redis.call("ZADD", queueTotalIndexKey, "NX", currentTime, queueKey)

return redis.call("SPUBLISH", eventChannelKey, eventPayload) > 0
