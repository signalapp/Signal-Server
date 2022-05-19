local queueKey     = KEYS[1]
local queueLockKey = KEYS[2]
local limit        = ARGV[1]

local locked = redis.call("GET", queueLockKey)

if locked then
    return {}
end

-- The range is inclusive
local min = 0
local max = limit - 1

if max < 0 then
    return {}
end

return redis.call("ZRANGE", queueKey, min, max, "WITHSCORES")
