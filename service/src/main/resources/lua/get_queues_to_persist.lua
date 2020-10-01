local queueTotalIndexKey = KEYS[1]
local maxTime            = ARGV[1]
local limit              = ARGV[2]

local results = redis.call("ZRANGEBYSCORE", queueTotalIndexKey, 0, maxTime, "LIMIT", 0, limit)

if results and next(results) then
    redis.call("ZREM", queueTotalIndexKey, unpack(results))
end

return results
