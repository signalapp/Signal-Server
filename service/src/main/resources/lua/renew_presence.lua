local presenceKey  = KEYS[1]
local presenceUuid = ARGV[1]
local expireSeconds = ARGV[2]

if redis.call("GET", presenceKey) == presenceUuid then
    redis.call("EXPIRE", presenceKey, expireSeconds)
end
