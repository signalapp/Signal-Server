local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]
local message            = ARGV[1]
local currentTime        = ARGV[2]
local sender             = ARGV[3]
local guid               = ARGV[4]

if redis.call("HEXISTS", queueMetadataKey, guid) == 1 then
    return tonumber(redis.call("HGET", queueMetadataKey, guid))
end

local messageId = redis.call("HINCRBY", queueMetadataKey, "counter", 1)

redis.call("ZADD", queueKey, "NX", messageId, message)

if sender ~= "nil" then
    redis.call("HSET", queueMetadataKey, sender, messageId)
    redis.call("HSET", queueMetadataKey, messageId, sender)
end

redis.call("HSET", queueMetadataKey, guid, messageId)
redis.call("HSET", queueMetadataKey, messageId .. "guid", guid)

redis.call("EXPIRE", queueKey, 7776000)         -- 90 days
redis.call("EXPIRE", queueMetadataKey, 7776000) -- 90 days

redis.call("ZADD", queueTotalIndexKey, "NX", currentTime, queueKey)
return messageId
