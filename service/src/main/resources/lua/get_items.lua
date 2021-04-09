local queueKey     = KEYS[1]
local queueLockKey = KEYS[2]
local limit        = ARGV[1]

local locked = redis.call("GET", queueLockKey)

if locked then
    return {}
end

return redis.call("ZRANGE", queueKey, 0, limit, "WITHSCORES")
