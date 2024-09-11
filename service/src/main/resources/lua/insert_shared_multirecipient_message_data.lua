-- inserts shared multi-recipient message data

local sharedMrmKey = KEYS[1] -- [string] the key containing the shared MRM data
local mrmData      = ARGV[1] -- [bytes] the serialized multi-recipient message data
-- the remainder of ARGV is list of recipient keys and view data

redis.call("HSET", sharedMrmKey, "data", mrmData);
redis.call("EXPIRE", sharedMrmKey, 604800) -- 7 days

-- unpack() fails with "too many results" at very large table sizes, so we loop
for i = 2, #ARGV, 2 do
    redis.call("HSET", sharedMrmKey, ARGV[i], ARGV[i + 1])
end
