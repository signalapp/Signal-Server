-- keys: queue_key, queue_locked_key
-- argv: limit

local locked = redis.call("GET", KEYS[2])

if locked then
    return {}
end

return redis.call("ZRANGE", KEYS[1], 0, ARGV[1], "WITHSCORES")