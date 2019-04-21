-- keys: queue KEYS[1], endpoint (KEYS[2])

redis.call("DEL", KEYS[2])
return redis.call("ZREM", KEYS[1], KEYS[2])
