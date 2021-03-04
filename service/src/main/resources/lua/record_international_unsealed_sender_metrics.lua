local destinationSetKey = KEYS[1]
local messageCountKey = KEYS[2]

local destination = ARGV[1]

redis.call("PFADD", destinationSetKey, destination)
local distinctDestinationCount = redis.call("PFCOUNT", destinationSetKey)

if redis.call("TTL", destinationSetKey) < 0 then
    redis.call("EXPIRE", destinationSetKey, 86400)
end

local messageCount = redis.call("INCR", messageCountKey)

if messageCount == 1 then
    redis.call("EXPIRE", messageCountKey, 86400)
end

return { distinctDestinationCount, messageCount }
