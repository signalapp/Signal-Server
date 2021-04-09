local presenceKey  = KEYS[1]
local presenceUuid = ARGV[1]

if redis.call("GET", presenceKey) == presenceUuid then
    redis.call("DEL", presenceKey)
    return true
end

return false
