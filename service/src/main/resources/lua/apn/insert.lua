local pendingNotificationQueue = KEYS[1]
local endpoint = KEYS[2]

local timestamp = ARGV[1]
local interval = ARGV[2]
local account = ARGV[3]
local deviceId = ARGV[4]

redis.call("HSET", endpoint, "created", timestamp)
redis.call("HSET", endpoint, "interval", interval)
redis.call("HSET", endpoint, "account", account)
redis.call("HSET", endpoint, "device", deviceId)

redis.call("ZADD", pendingNotificationQueue, timestamp, endpoint)
