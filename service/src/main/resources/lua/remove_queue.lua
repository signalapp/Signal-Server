local queueKey           = KEYS[1]
local queueMetadataKey   = KEYS[2]
local queueTotalIndexKey = KEYS[3]

redis.call("DEL", queueKey)
redis.call("DEL", queueMetadataKey)
redis.call("ZREM", queueTotalIndexKey, queueKey)
