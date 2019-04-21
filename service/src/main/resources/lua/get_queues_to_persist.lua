-- keys: queue_total_index
-- argv: max_time, limit

local results = redis.call("ZRANGEBYSCORE", KEYS[1], 0, ARGV[1], "LIMIT", 0, ARGV[2])

if results and next(results) then
    redis.call("ZREM", KEYS[1], unpack(results))
end

return results
