local queueKey     = KEYS[1]
local queueLockKey = KEYS[2]
local limit        = ARGV[1]
local afterMessageId = ARGV[2]

local locked = redis.call("GET", queueLockKey)

if locked then
    return {}
end

if afterMessageId == "null" then
    -- An index range is inclusive
    local min = 0
    local max = limit - 1

    if max < 0 then
        return {}
    end

    return redis.call("ZRANGE", queueKey, min, max, "WITHSCORES")
else
    -- note: this is deprecated in Redis 6.2, and should be migrated to zrange after the cluster is updated
    return redis.call("ZRANGEBYSCORE", queueKey, "("..afterMessageId, "+inf", "WITHSCORES", "LIMIT", 0, limit)
end
