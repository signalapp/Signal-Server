local pendingNotificationQueue = KEYS[1]
local endpoint = KEYS[2]

redis.call("DEL", endpoint)
return redis.call("ZREM", pendingNotificationQueue, endpoint)
