-- returns a list of queues that meet persistence criteria

local queueTotalIndexKey = KEYS[1] -- sorted set of all queues in the shard, by timestamp of oldest message
local maxTime            = ARGV[1] -- [number] the most recent queue timestamp that may be fetched
local limit              = ARGV[2] -- [number] the maximum number of queues to fetch

local results = redis.call("ZRANGE", queueTotalIndexKey, 0, maxTime, "BYSCORE", "LIMIT", 0, limit)

if results and next(results) then
    redis.call("ZREM", queueTotalIndexKey, unpack(results))
end

return results
