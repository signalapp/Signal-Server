-- Unlocks a message queue when a persist-to-DynamoDB run has finished and publishes an event notifying listeners that
-- messages have been persisted

local persistInProgressKey = KEYS[1] -- simple string key whose presence indicates a lock
local eventChannelKey      = KEYS[2] -- the channel on which to publish the "messages persisted" event
local eventPayload         = ARGV[1] -- [bytes] a protobuf payload for a "message persisted" pub/sub event

redis.call("DEL", persistInProgressKey)
redis.call("SPUBLISH", eventChannelKey, eventPayload)
