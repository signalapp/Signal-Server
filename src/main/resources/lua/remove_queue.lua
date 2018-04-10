-- keys: queue_key, queue_metadata_key, queue_index

redis.call("DEL", KEYS[1])
redis.call("DEL", KEYS[2])
redis.call("ZREM", KEYS[3], KEYS[1])
