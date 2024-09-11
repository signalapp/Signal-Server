-- gets messages from a device's queue, up to a given limit
-- returns a list of all envelopes and their queue-local IDs

local queueKey       = KEYS[1] -- sorted set of all Envelopes for a device, scored by queue-local ID
local queueLockKey   = KEYS[2] -- a key whose presence indicates that the queue is being persistent and must not be read
local limit          = ARGV[1] -- [number] the maximum number of messages to return
local afterMessageId = ARGV[2] -- [number] a queue-local ID to exclusively start after, to support pagination. Use -1 to start at the beginning

local locked = redis.call("GET", queueLockKey)

if locked then
    return {}
end

if afterMessageId == "null" or afterMessageId == nil then
    return redis.error_reply("ERR afterMessageId is required")
end

return redis.call("ZRANGE", queueKey, "("..afterMessageId, "+inf", "BYSCORE", "LIMIT", 0, limit, "WITHSCORES")
