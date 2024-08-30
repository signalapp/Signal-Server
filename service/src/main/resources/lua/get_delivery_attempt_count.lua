local firstMessageGuidKey = KEYS[1]
local firstMessageAttemptsKey = KEYS[2]

local firstMessageGuid = ARGV[1]
local ttlSeconds = ARGV[2]

if firstMessageGuid ~= redis.call("GET", firstMessageGuidKey) then
    -- This is the first time we've attempted to deliver this message as the first message in a "page"
    redis.call("SET", firstMessageGuidKey, firstMessageGuid, "EX", ttlSeconds)
    redis.call("SET", firstMessageAttemptsKey, 0, "EX", ttlSeconds)
end

return redis.call("INCR", firstMessageAttemptsKey)
